"""Command-line entry point for mdm_merge.

Usage (via ./mdm at repo root, or ``python -m mdm_merge.cli``):

  mdm init DB [--replica-id ID]
  mdm batch  DB batch.json
  mdm checkpoint DB supplier out.json
  mdm delta DB supplier baseline.json out.json
  mdm merge DB package.json
  mdm get DB supplier asset_id
  mdm list DB supplier
  mdm suppliers DB
  mdm stats DB
  mdm compact DB
  mdm inspect package.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict

from .core import MergeError, Replica
from .protocol import ProtocolError, load_handoff_file
from .subscriptions import (SubscriptionError,
                            SubscriptionManager)
from .landing import LandingError, LandingStore


def _emit(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _token_arg(text: str) -> str:
    """Accept a raw token or a path to a page file carrying next_token."""
    if os.path.exists(text):
        with open(text, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
        token = doc.get("next_token")
        if not token:
            raise SubscriptionError(
                f"{text}: page has no next_token (stream reached its end)")
        return token
    return text


def _write_page(page: Dict[str, Any], out: str | None) -> None:
    if out:
        from .protocol import write_json_atomic
        write_json_atomic(out, page)
    else:
        _emit(page)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mdm",
                                description="Offline multi-replica master-data merge")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="create a replica database")
    sp.add_argument("db")
    sp.add_argument("--replica-id")

    sp = sub.add_parser("batch", help="import a local put/erase batch file")
    sp.add_argument("db")
    sp.add_argument("batch_file")

    sp = sub.add_parser("checkpoint", help="export full supplier checkpoint")
    sp.add_argument("db")
    sp.add_argument("supplier")
    sp.add_argument("out")

    sp = sub.add_parser("delta", help="export delta vs an exported checkpoint")
    sp.add_argument("db")
    sp.add_argument("supplier")
    sp.add_argument("baseline")
    sp.add_argument("out")

    sp = sub.add_parser("merge", help="merge a handoff package")
    sp.add_argument("db")
    sp.add_argument("package")

    sp = sub.add_parser("get", help="read one row with per-field live versions")
    sp.add_argument("db")
    sp.add_argument("supplier")
    sp.add_argument("asset_id")

    sp = sub.add_parser("list", help="list live rows of a supplier partition")
    sp.add_argument("db")
    sp.add_argument("supplier")

    sp = sub.add_parser("suppliers", help="list partitions this replica knows")
    sp.add_argument("db")

    sp = sub.add_parser("stats", help="live versions, summaries, disk usage")
    sp.add_argument("db")

    sp = sub.add_parser("compact", help="collapse provenance into summaries")
    sp.add_argument("db")

    sp = sub.add_parser("inspect", help="validate and summarize a package")
    sp.add_argument("package")

    # --------------------------------------------------- subscription outlet
    sp = sub.add_parser("sub-create", help="create a resumable downstream "
                                           "subscription at one cut point")
    sp.add_argument("db")
    sp.add_argument("supplier")
    sp.add_argument("--page-size", type=int, default=1000)

    sp = sub.add_parser("sub-load", help="read one initial-load page by token")
    sp.add_argument("db")
    sp.add_argument("token_or_file")
    sp.add_argument("--out")

    sp = sub.add_parser("sub-ack-load", help="confirm a durable load page")
    sp.add_argument("db")
    sp.add_argument("token_or_file")

    sp = sub.add_parser("sub-complete", help="finish the load phase, open "
                                             "the change stream")
    sp.add_argument("db")
    sp.add_argument("sub_id")

    sp = sub.add_parser("sub-events", help="read one change-event page")
    sp.add_argument("db")
    sp.add_argument("token_or_file")
    sp.add_argument("--page-size", type=int, default=100)
    sp.add_argument("--out")

    sp = sub.add_parser("sub-ack-events", help="confirm a durable event "
                                               "position")
    sp.add_argument("db")
    sp.add_argument("token_or_file")

    sp = sub.add_parser("sub-backlog", help="show subscription backlog and "
                                            "retention state")
    sp.add_argument("db")
    sp.add_argument("--sub-id")

    sp = sub.add_parser("sub-release", help="release a subscription (future "
                                            "consumers must reload)")
    sp.add_argument("db")
    sp.add_argument("sub_id")

    sp = sub.add_parser("sub-cleanup", help="reclaim unprotected change "
                                            "history and load copies")
    sp.add_argument("db")

    # ------------------------------------------------- landing reference end
    sp = sub.add_parser("land-begin", help="start/replace a full load in a "
                                           "landing database")
    sp.add_argument("landing_db")
    sp.add_argument("page_file")

    sp = sub.add_parser("land-apply-load", help="apply one load page to a "
                                                "landing database")
    sp.add_argument("landing_db")
    sp.add_argument("page_file")

    sp = sub.add_parser("land-complete", help="publish a finished load "
                                              "atomically")
    sp.add_argument("landing_db")
    sp.add_argument("supplier")

    sp = sub.add_parser("land-apply-events", help="apply one event page "
                                                  "atomically")
    sp.add_argument("landing_db")
    sp.add_argument("page_file")

    sp = sub.add_parser("land-status", help="show landing bindings and "
                                            "positions")
    sp.add_argument("landing_db")

    sp = sub.add_parser("land-get", help="read one landed row")
    sp.add_argument("landing_db")
    sp.add_argument("supplier")
    sp.add_argument("asset_id")

    sp = sub.add_parser("land-list", help="list landed rows of a partition")
    sp.add_argument("landing_db")
    sp.add_argument("supplier")

    args = p.parse_args(argv)

    try:
        if args.cmd == "init":
            rep = Replica.create(args.db, args.replica_id)
            _emit({"created": args.db, "replica_id": rep.replica_id})
            rep.close()
            return 0

        if args.cmd == "inspect":
            pkg = load_handoff_file(args.package)
            _emit({"kind": pkg["kind"], "replica_id": pkg["replica_id"],
                   "supplier": pkg["supplier"],
                   "checkpoint_id": pkg["checkpoint_id"],
                   "frontier": pkg["frontier"],
                   "add_records": len(pkg["adds"]),
                   "kill_records": len(pkg["kills"]),
                   "baseline": pkg.get("baseline")})
            return 0

        # The independent landing end never opens the source replica file.
        if args.cmd.startswith("land-"):
            return _run_subscription_or_landing(args)

        with Replica(args.db) as rep:
            if args.cmd == "batch":
                counters = rep.import_batch_file(args.batch_file)
                _emit({"imported_ops": len(counters), "counters": counters,
                       "replica_id": rep.replica_id})
            elif args.cmd == "checkpoint":
                cid = rep.export_checkpoint(args.supplier, args.out)
                _emit({"checkpoint_id": cid, "path": args.out,
                       "supplier": args.supplier})
            elif args.cmd == "delta":
                with open(args.baseline, "r", encoding="utf-8") as fh:
                    baseline = json.load(fh)
                cid = rep.export_delta(args.supplier, baseline, args.out)
                _emit({"delta_id": cid, "path": args.out,
                       "baseline": baseline.get("checkpoint_id")})
            elif args.cmd == "merge":
                result = rep.merge_file(args.package)
                _emit({"merged": result, "replica_id": rep.replica_id})
            elif args.cmd == "get":
                _emit(rep.read_row(args.supplier, args.asset_id))
            elif args.cmd == "list":
                _emit(rep.read_partition(args.supplier))
            elif args.cmd == "suppliers":
                _emit({"suppliers": rep.list_suppliers()})
            elif args.cmd == "stats":
                _emit(rep.stats())
            elif args.cmd == "compact":
                _emit({"compacted": rep.compact(), "stats": rep.stats()})
            else:
                return _run_subscription_or_landing(args)
    except (ProtocolError, MergeError, SubscriptionError, LandingError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


def _load_page_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _run_subscription_or_landing(args: argparse.Namespace) -> int:
    cmd = args.cmd
    if cmd == "sub-create":
        with SubscriptionManager(args.db) as mgr:
            _emit(mgr.create_subscription(args.supplier, args.page_size))
        return 0
    if cmd == "sub-load":
        token = _token_arg(args.token_or_file)
        with SubscriptionManager(args.db) as mgr:
            _write_page(mgr.read_load_page(token), args.out)
        return 0
    if cmd == "sub-ack-load":
        token = _token_arg(args.token_or_file)
        with SubscriptionManager(args.db) as mgr:
            _emit(mgr.ack_load_page(token))
        return 0
    if cmd == "sub-complete":
        with SubscriptionManager(args.db) as mgr:
            _emit(mgr.complete_load(args.sub_id))
        return 0
    if cmd == "sub-events":
        token = _token_arg(args.token_or_file)
        with SubscriptionManager(args.db) as mgr:
            _write_page(mgr.read_event_page(token, args.page_size),
                        args.out)
        return 0
    if cmd == "sub-ack-events":
        token = _token_arg(args.token_or_file)
        with SubscriptionManager(args.db) as mgr:
            _emit(mgr.ack_events(token))
        return 0
    if cmd == "sub-backlog":
        with SubscriptionManager(args.db) as mgr:
            _emit(mgr.backlog(args.sub_id))
        return 0
    if cmd == "sub-release":
        with SubscriptionManager(args.db) as mgr:
            _emit(mgr.release_subscription(args.sub_id))
        return 0
    if cmd == "sub-cleanup":
        with SubscriptionManager(args.db) as mgr:
            _emit(mgr.cleanup())
        return 0
    # landing end: an independent database, never the source replica file
    if cmd == "land-begin":
        with LandingStore(args.landing_db) as land:
            _emit(land.begin_load(_load_page_file(args.page_file)))
        return 0
    if cmd == "land-apply-load":
        with LandingStore(args.landing_db) as land:
            _emit(land.apply_load_page(_load_page_file(args.page_file)))
        return 0
    if cmd == "land-complete":
        with LandingStore(args.landing_db) as land:
            _emit(land.complete_load(args.supplier))
        return 0
    if cmd == "land-apply-events":
        with LandingStore(args.landing_db) as land:
            _emit(land.apply_event_page(_load_page_file(args.page_file)))
        return 0
    if cmd == "land-status":
        with LandingStore(args.landing_db) as land:
            _emit(land.status())
        return 0
    if cmd == "land-get":
        with LandingStore(args.landing_db) as land:
            _emit(land.read_row(args.supplier, args.asset_id))
        return 0
    if cmd == "land-list":
        with LandingStore(args.landing_db) as land:
            _emit({"rows": land.list_rows(args.supplier)})
        return 0
    raise SubscriptionError(f"unknown command {cmd!r}")


if __name__ == "__main__":
    raise SystemExit(main())
