"""Reference causal-graph oracle for acceptance testing only.

This is deliberately a *separate*, naive implementation: it keeps the full
operation history and answers liveness directly from the rule

    a new operation deletes exactly the field versions it observed

It never imports or calls the production merge/compaction code, so agreement
between the two is genuine evidence rather than shared logic.

Operation coordinates are (replica_id, counter).  A local op observes the
live versions its put/erase replaced at that replica.  Handoff packages are
treated exactly like their underlying add/kill facts.  Two structural rules
mirror the production wire format exactly:

* a kill record is a *prefix tombstone* -- per (row, field, creator),
  "every counter <= N has been observed and is dead" (watermark);
* a remote tombstone only takes effect once its target add fact is known
  here, otherwise it is buffered until that add arrives (OR-set rule), so
  delta-before-baseline delivery cannot resurrect or pre-kill anything.

Delivery order cannot change the result because every rule only ever adds
facts to union structures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

FIELDS = ("location", "model", "category")
Key = Tuple[str, str, str]                  # supplier, asset_id, field
Vid = Tuple[str, int, str]                  # creator, counter, field
WmKey = Tuple[str, str, str, str]           # supplier, asset_id, field, creator


@dataclass
class OracleReplica:
    replica_id: str
    counter: int = 0
    # version -> (supplier, asset_id, value); holds the newest known add per
    # (row, field, creator), mirroring field_state
    versions: Dict[Vid, Tuple[str, str, Optional[str]]] = field(default_factory=dict)
    # prefix tombstones already applicable: counter per (row,field,creator)
    watermark: Dict[WmKey, int] = field(default_factory=dict)
    # folded value of the watermarked version, mirroring dead_watermark
    # dead_value; one entry per (row, field, creator), bounded by touched keys
    dead_values: Dict[WmKey, Optional[str]] = field(default_factory=dict)
    # compact dead-coordinate summary mirroring dead_watermark.dead_blob:
    # key -> {counter: value} for retained dead coordinates
    dead_blobs: Dict[WmKey, Dict[int, Optional[str]]] = field(default_factory=dict)
    # tombstones whose exact add fact has not arrived yet; one set of
    # counters per (row, field, creator) so distinct coordinates coexist
    pending: Dict[WmKey, set] = field(default_factory=dict)
    # values of adds carried by the package currently being integrated
    _incoming_values: Dict[Vid, Tuple[str, str, Optional[str]]] = \
        field(default_factory=dict)
    # causal frontier per replica, as declared by integrated packages
    frontier: Dict[str, int] = field(default_factory=dict)
    # full history of integrated local coordinates (diagnostics only)
    history: List[Tuple[str, int]] = field(default_factory=list)

    # ------------------------------------------------------------- helpers
    def _is_dead(self, vid: Vid) -> bool:
        creator, counter, fld = vid
        s, a, _v = self.versions[vid]
        return self.watermark.get((s, a, fld, creator), 0) >= counter

    def _raise_watermark(self, supplier: str, asset_id: str, fld: str,
                         creator: str, counter: int,
                         value: Optional[str] = None) -> None:
        k = (supplier, asset_id, fld, creator)
        old = self.watermark.get(k, 0)
        if counter > old:
            self.watermark[k] = counter
            if value is None:
                vid = (creator, counter, fld)
                src = self.versions.get(vid, self._incoming_values.get(vid))
                value = src[2] if src is not None \
                    else self.dead_values.get(k)
            self.dead_values[k] = value
        # retain the dead coordinate fact regardless of tip movement
        blob = self.dead_blobs.setdefault(k, {})
        if counter not in blob:
            if value is None:
                vid = (creator, counter, fld)
                src = self.versions.get(vid, self._incoming_values.get(vid))
                value = src[2] if src is not None else self.dead_values.get(k)
            blob[counter] = value

    def _live(self, supplier: str, asset_id: str,
              fields: Optional[List[str]] = None) -> List[Vid]:
        out = []
        for vid, (s, a, _v) in self.versions.items():
            creator, counter, fld = vid
            if s != supplier or a != asset_id:
                continue
            if fields is not None and fld not in fields:
                continue
            if not self._is_dead(vid):
                out.append(vid)
        return out

    # ---------------------------------------------------------- local ops
    def local_put(self, supplier: str, asset_id: str,
                  fields: Dict[str, Optional[str]]) -> None:
        self.counter += 1
        observed = self._live(supplier, asset_id, list(fields))
        for fld, value in fields.items():
            vid = (self.replica_id, self.counter, fld)
            # One row per (row, field, creator): a new local put replaces
            # this creator's own prior row on that field.
            for old in [v for v in self.versions
                        if v[0] == self.replica_id and v[2] == fld
                        and self.versions[v][:2] == (supplier, asset_id)]:
                del self.versions[old]
            self.versions[vid] = (supplier, asset_id, value)
            for dead in observed:
                if dead[2] == fld:
                    self._raise_watermark(supplier, asset_id, fld,
                                          dead[0], dead[1])
        self.history.append((self.replica_id, self.counter))

    def local_erase(self, supplier: str, asset_id: str) -> None:
        self.counter += 1
        observed = self._live(supplier, asset_id)
        for dead in observed:
            creator, counter, fld = dead
            self._raise_watermark(supplier, asset_id, fld, creator, counter)
        self.history.append((self.replica_id, self.counter))

    def _drain(self) -> None:
        """Release buffered removes whose exact add fact has arrived.

        Each coordinate is judged independently; a higher chain version never
        releases a remove targeting a different (lower or unseen) counter.
        The add fact may be a current version, an incoming package add, or
        the folded dead_value recorded at this watermark key.
        """
        for k, counters in list(self.pending.items()):
            s, a, fld, creator = k

            def known(n):
                vid = (creator, n, fld)
                if vid in self.versions and \
                        self.versions[vid][:2] == (s, a):
                    return True
                if vid in self._incoming_values and \
                        self._incoming_values[vid][:2] == (s, a):
                    return True
                if n in self.dead_blobs.get(k, {}):
                    return True
                return self.watermark.get(k, 0) == n

            ready = {n for n in counters if known(n)}
            for n in ready:
                self._raise_watermark(s, a, fld, creator, n)
            remaining = counters - ready
            if remaining:
                self.pending[k] = remaining
            else:
                del self.pending[k]

    # ------------------------------------------------------ package facts
    def apply_handoff(self, pkg: Dict[str, Any]) -> None:
        # Causal frontier first (independent union summary, retained for
        # diagnostics; it never by itself releases a remove).
        for rid, cnt in pkg.get("frontier", {}).items():
            self.frontier[rid] = max(self.frontier.get(rid, 0), cnt)
        # Add facts carried by this package, available while its removes are
        # drained so a self-contained (add, remove) pair always matches.
        self._incoming_values = {
            (r["creator"], r["counter"], r["field"]):
                (r["supplier"], r["asset_id"], r["value"])
            for r in pkg["adds"]}
        for r in pkg["adds"]:
            vid = (r["creator"], r["counter"], r["field"])
            payload = (r["supplier"], r["asset_id"], r["value"])
            # Mirror field_state: one current row per (row, field, creator)
            # holding the highest known counter; lower coordinates are
            # discarded from the live map.
            existing = [v for v in self.versions
                        if v[0] == r["creator"] and v[2] == r["field"]
                        and self.versions[v][:2] ==
                        (r["supplier"], r["asset_id"])]
            if existing and max(v[1] for v in existing) >= r["counter"]:
                continue
            for v in existing:
                del self.versions[v]
            self.versions[vid] = payload
        # Buffer remote tombstones first, then release every one whose exact
        # add fact is now present (current version, incoming add, or folded
        # watermark value).
        for r in pkg["kills"]:
            k = (r["supplier"], r["asset_id"], r["field"], r["creator"])
            self.pending.setdefault(k, set()).add(r["counter"])
        self._drain()
        self._incoming_values = {}

    # ------------------------------------------------------------- compaction
    def compact(self, keep: int = 64) -> None:
        """Mirror production blob pruning; liveness results are unchanged for
        retained coordinates and the prefix watermark still covers older ones."""
        for k, blob in list(self.dead_blobs.items()):
            if len(blob) > keep:
                kept = dict(sorted(blob.items())[-keep:])
                self.dead_blobs[k] = kept

    # ------------------------------------------------------------- reads
    def live_versions(self, supplier: str, asset_id: str,
                      fld: str) -> List[Tuple[str, int, Optional[str]]]:
        out = []
        for vid, (s, a, value) in self.versions.items():
            creator, counter, f = vid
            if (s, a, f) == (supplier, asset_id, fld) and not self._is_dead(vid):
                out.append((creator, counter, value))
        return sorted(out, key=lambda x: (x[0], x[1]))

    def snapshot(self) -> Dict[Key, List[Tuple[str, int, Optional[str]]]]:
        rows: Dict[Key, List[Tuple[str, int, Optional[str]]]] = {}
        for vid, (s, a, value) in self.versions.items():
            creator, counter, fld = vid
            if not self._is_dead(vid):
                rows.setdefault((s, a, fld), []).append(
                    (creator, counter, value))
        for v in rows.values():
            v.sort(key=lambda x: (x[0], x[1]))
        return rows
