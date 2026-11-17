"""Protocol validation tests: malformed input is rejected wholesale.

Covers unknown fields/ops, bad scalar types, out-of-scope records, truncated
or tampered packages, bad digest and unknown protocol version. A rejected
input must never partially mutate the database.
"""

from __future__ import annotations

import copy
import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mdm_merge import (Replica, ProtocolError, MergeError,   # noqa: E402
                       build_handoff, protocol)

FAIL = []


def check(label, cond):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        FAIL.append(label)


def expect_protocol_error(label, fn):
    try:
        fn()
    except (ProtocolError, MergeError):
        check(label, True)
        return
    check(label, False)


def main():
    work = tempfile.mkdtemp(prefix="mdm-proto-")
    rep = Replica.create(os.path.join(work, "r.db"), "R")
    good = {"format": "mdm-batch/v1", "protocol_version": 1, "ops": [
        {"op": "put", "supplier": "S", "asset_id": "a",
         "fields": {"location": "x"}}]}

    expect_protocol_error("unknown field name rejected",
                          lambda: protocol.validate_batch(
                              {**good, "ops": [{"op": "put", "supplier": "S",
                                "asset_id": "a",
                                "fields": {"bogus": "x"}}]}))
    expect_protocol_error("non-string scalar rejected",
                          lambda: protocol.validate_batch(
                              {**good, "ops": [{"op": "put", "supplier": "S",
                                "asset_id": "a",
                                "fields": {"location": 3}}]}))
    expect_protocol_error("unknown op kind rejected",
                          lambda: protocol.validate_batch(
                              {**good, "ops": [{"op": "delete", "supplier": "S",
                                "asset_id": "a"}]}))
    expect_protocol_error("empty ops rejected",
                          lambda: protocol.validate_batch(
                              {**good, "ops": []}))
    expect_protocol_error("unknown protocol version rejected",
                          lambda: protocol.validate_batch(
                              {**good, "protocol_version": 99}))
    expect_protocol_error("non-document batch rejected",
                          lambda: protocol.validate_batch(["nope"]))

    rep.import_batch(good["ops"])
    cp_path = os.path.join(work, "cp.json")
    rep.export_checkpoint("S", cp_path)
    pkg = protocol.load_handoff_file(cp_path)
    before = rep.stats()

    # tamper with a value but keep the old digest
    tampered = copy.deepcopy(pkg)
    if tampered["adds"]:
        tampered["adds"][0]["value"] = "TAMPERED"
    expect_protocol_error("tampered content digest rejected",
                          lambda: rep.merge_package(tampered))

    # wrong digest field
    bad_digest = copy.deepcopy(pkg)
    bad_digest["digest"] = "0" * 64
    expect_protocol_error("forged digest rejected",
                          lambda: rep.merge_package(bad_digest))

    # record escapes supplier scope
    esc = copy.deepcopy(pkg)
    if esc["adds"]:
        esc["adds"][0]["supplier"] = "OTHER"
        esc["digest"] = protocol.compute_digest(esc)
    expect_protocol_error("cross-supplier record rejected at merge",
                          lambda: rep.merge_package(esc))

    # truncated file
    trunc = os.path.join(work, "trunc.json")
    raw = open(cp_path, "rb").read()
    with open(trunc, "wb") as fh:
        fh.write(raw[: len(raw) // 2])
    expect_protocol_error("truncated package rejected",
                          lambda: rep.merge_file(trunc))

    # unknown extra top-level field is tolerated only if digest covers it;
    # injecting one without re-signing must fail the digest
    extra = copy.deepcopy(pkg)
    extra["unexpected"] = 123
    expect_protocol_error("unsigned structural change rejected",
                          lambda: protocol.validate_handoff(extra))

    after = rep.stats()
    check("rejected inputs caused no state change",
          before == after)

    # idempotency: merging the valid package twice changes nothing extra
    rep.merge_file(cp_path)
    once = rep.stats()
    rep.merge_file(cp_path)
    twice = rep.stats()
    check("duplicate valid package merge is idempotent", once == twice)

    # empty delta (no changes vs baseline) must validate and merge
    empty_delta = build_handoff(
        kind="delta", replica_id="R", supplier="S",
        frontier=pkg["frontier"], adds=[], kills=[],
        checkpoint_id="dl-empty",
        baseline={"checkpoint_id": pkg["checkpoint_id"],
                  "frontier": pkg["frontier"]})
    rep.merge_package(empty_delta)
    check("empty delta accepted and harmless", rep.stats() == twice)

    rep.close()
    if FAIL:
        print(f"\n{len(FAIL)} PROTOCOL FAILURE(S)")
        sys.exit(1)
    print("\nAll protocol validation checks passed.")


if __name__ == "__main__":
    main()
