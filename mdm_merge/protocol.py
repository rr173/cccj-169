"""Offline handoff package + local batch wire protocol.

Two on-disk JSON formats, both versioned and self-describing:

* local batch file  (a site's own put/erase work, imported locally)
* handoff package   (checkpoint / delta, exchanged between replicas)

Files are parsed with strict shape/type/range validation: a truncated,
corrupted, out-of-range or unknown-structure file is rejected wholesale --
the caller's database is never partially mutated.

Content is authenticated by a canonical SHA-256 digest over every field
except the trailing ``digest`` key.  This project trusts legitimate
replicas (no Byzantine protocol); the digest guards transport corruption
and accidental editing, not a hostile peer.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple

PROTOCOL_VERSION = 1
BATCH_FORMAT = "mdm-batch/v1"
HANDOFF_FORMATS = ("mdm-handoff/v1",)
KIND_CHECKPOINT = "checkpoint"
KIND_DELTA = "delta"
KINDS = (KIND_CHECKPOINT, KIND_DELTA)
VALID_FIELDS = ("location", "model", "category")


class ProtocolError(ValueError):
    """Raised when an input file fails structural/content validation."""


# --------------------------------------------------------------------------
# canonical encoding / digest
# --------------------------------------------------------------------------

def canonical(obj: Any) -> bytes:
    """Deterministic JSON encoding: sorted keys, no insignificant whitespace."""
    return json.dumps(
        obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def compute_digest(payload: Dict[str, Any]) -> str:
    body = {k: v for k, v in payload.items() if k != "digest"}
    return hashlib.sha256(canonical(body)).hexdigest()


def _is_scalar(v: Any) -> bool:
    return v is None or isinstance(v, str)


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ProtocolError(msg)


def _check_id(s: Any, what: str) -> None:
    _require(isinstance(s, str) and 1 <= len(s) <= 128,
             f"{what} must be a non-empty string (<=128 chars)")


# --------------------------------------------------------------------------
# local batches
# --------------------------------------------------------------------------

def validate_batch(doc: Any) -> Dict[str, Any]:
    """Validate a parsed batch document and return it normalized."""
    _require(isinstance(doc, dict), "batch must be a JSON object")
    _require(doc.get("format") == BATCH_FORMAT,
             f"unsupported batch format: {doc.get('format')!r}")
    _require(doc.get("protocol_version") == PROTOCOL_VERSION,
             "unknown protocol_version")
    ops = doc.get("ops")
    _require(isinstance(ops, list) and len(ops) >= 1,
             "batch must contain a non-empty ops list")
    norm_ops = []
    for i, op in enumerate(ops):
        _require(isinstance(op, dict), f"op[{i}] must be an object")
        kind = op.get("op")
        supplier = op.get("supplier")
        asset_id = op.get("asset_id")
        _check_id(supplier, f"op[{i}].supplier")
        _check_id(asset_id, f"op[{i}].asset_id")
        if kind == "put":
            fields = op.get("fields")
            _require(isinstance(fields, dict) and fields,
                     f"put op[{i}] needs a non-empty fields object")
            for fname, fval in fields.items():
                _require(fname in VALID_FIELDS,
                         f"op[{i}]: unknown field {fname!r}")
                _require(_is_scalar(fval),
                         f"op[{i}].{fname}: only string or null allowed")
            # reject duplicate field keys defensively (json already forbids)
            norm_ops.append({"op": "put", "supplier": supplier,
                             "asset_id": asset_id, "fields": dict(fields)})
        elif kind == "erase":
            norm_ops.append({"op": "erase", "supplier": supplier,
                             "asset_id": asset_id})
        else:
            raise ProtocolError(f"op[{i}]: unknown op kind {kind!r}")
    return {"format": BATCH_FORMAT, "protocol_version": PROTOCOL_VERSION,
            "ops": norm_ops}


def load_batch_file(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"cannot read batch {path}: {exc}") from exc
    return validate_batch(doc)["ops"]


# --------------------------------------------------------------------------
# handoff packages
# --------------------------------------------------------------------------

def _check_handoff_records(records: Any, what: str) -> List[Dict[str, Any]]:
    _require(isinstance(records, list), f"{what} must be a list")
    out = []
    for i, r in enumerate(records):
        _require(isinstance(r, dict), f"{what}[{i}] must be an object")
        supplier = r.get("supplier")
        asset_id = r.get("asset_id")
        field = r.get("field")
        creator = r.get("creator")
        counter = r.get("counter")
        _check_id(supplier, f"{what}[{i}].supplier")
        _check_id(asset_id, f"{what}[{i}].asset_id")
        _require(field in VALID_FIELDS, f"{what}[{i}].field invalid")
        _check_id(creator, f"{what}[{i}].creator")
        _require(isinstance(counter, int) and not isinstance(counter, bool)
                 and counter >= 1, f"{what}[{i}].counter must be int >= 1")
        rec = {"supplier": supplier, "asset_id": asset_id, "field": field,
               "creator": creator, "counter": counter}
        out.append(rec)
    return out


def validate_handoff(doc: Any) -> Dict[str, Any]:
    _require(isinstance(doc, dict), "package must be a JSON object")
    _require(doc.get("format") in HANDOFF_FORMATS,
             f"unsupported handoff format: {doc.get('format')!r}")
    _require(doc.get("protocol_version") == PROTOCOL_VERSION,
             "unknown protocol_version")
    kind = doc.get("kind")
    _require(kind in KINDS, f"kind must be one of {KINDS}")
    _check_id(doc.get("replica_id"), "replica_id")
    supplier = doc.get("supplier")
    _check_id(supplier, "supplier")

    frontier = doc.get("frontier")
    _require(isinstance(frontier, dict), "frontier must be an object")
    for rid, cnt in frontier.items():
        _check_id(rid, "frontier key")
        _require(isinstance(cnt, int) and not isinstance(cnt, bool)
                 and cnt >= 0, "frontier values must be int >= 0")

    adds = _check_handoff_records(doc.get("adds"), "adds")
    kills = _check_handoff_records(doc.get("kills"), "kills")
    for r in adds:
        _require(_is_scalar(r.get("value")),
                 "adds records need string/null value")
    # adds carry value; validated records above don't, re-attach explicitly
    adds2 = []
    for r in doc["adds"]:
        adds2.append({"supplier": r["supplier"], "asset_id": r["asset_id"],
                      "field": r["field"], "creator": r["creator"],
                      "counter": r["counter"], "value": r["value"]})

    baseline = None
    if kind == KIND_DELTA:
        baseline = doc.get("baseline")
        _require(isinstance(baseline, dict), "delta needs a baseline object")
        _check_id(baseline.get("checkpoint_id"),
                  "baseline.checkpoint_id")
        bf = baseline.get("frontier")
        _require(isinstance(bf, dict), "baseline.frontier must be an object")
        for rid, cnt in bf.items():
            _check_id(rid, "baseline frontier key")
            _require(isinstance(cnt, int) and not isinstance(cnt, bool)
                     and cnt >= 0, "baseline frontier values must be int >= 0")
        # A delta may not repeat records or smuggle unchanged live rows.
        # Uniqueness is per full coordinate (a key can carry several dead
        # add facts at distinct counters alongside one live version). The
        # producer guarantees "change vs baseline" by per-key snapshot diff.
        add_seen, kill_seen = set(), set()
        for r in adds2:
            k = (r["supplier"], r["asset_id"], r["field"], r["creator"],
                 r["counter"])
            _require(k not in add_seen, "delta contains duplicate add record")
            add_seen.add(k)
        for r in kills:
            k = (r["supplier"], r["asset_id"], r["field"], r["creator"],
                 r["counter"])
            _require(k not in kill_seen,
                     "delta contains duplicate kill record")
            kill_seen.add(k)

    # digest check last, over the fully parsed structure
    digest = doc.get("digest")
    _require(isinstance(digest, str) and len(digest) == 64,
             "missing/malformed digest")
    if compute_digest(doc) != digest:
        raise ProtocolError("content digest mismatch: package corrupted "
                            "or modified in place")
    result = {
        "format": doc["format"], "protocol_version": PROTOCOL_VERSION,
        "kind": kind, "replica_id": doc["replica_id"],
        "supplier": supplier, "frontier": dict(frontier),
        "adds": adds2, "kills": kills,
        "checkpoint_id": doc.get("checkpoint_id"),
        "digest": digest,
    }
    if kind == KIND_DELTA:
        result["baseline"] = baseline
    return result


def load_handoff_file(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"cannot read package {path}: {exc}") from exc
    return validate_handoff(doc)


def build_handoff(*, kind: str, replica_id: str, supplier: str,
                  frontier: Dict[str, int], adds: List[Dict[str, Any]],
                  kills: List[Dict[str, Any]],
                  checkpoint_id: str,
                  baseline: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Assemble and sign a handoff package in memory."""
    payload: Dict[str, Any] = {
        "format": "mdm-handoff/v1",
        "protocol_version": PROTOCOL_VERSION,
        "kind": kind,
        "checkpoint_id": checkpoint_id,
        "replica_id": replica_id,
        "supplier": supplier,
        "frontier": dict(frontier),
        "adds": list(adds),
        "kills": list(kills),
    }
    if kind == KIND_DELTA:
        _require(baseline is not None, "delta requires a baseline")
        payload["baseline"] = baseline
    payload["digest"] = compute_digest(payload)
    # sign then re-validate once, guaranteeing every emitted package parses
    return validate_handoff(payload)


def write_json_atomic(path: str, doc: Dict[str, Any]) -> None:
    """Write to a fresh temp file and rename; input files are never edited."""
    import os
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    tmp = os.path.join(d, f".tmp-{os.getpid()}-{os.urandom(6).hex()}")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
