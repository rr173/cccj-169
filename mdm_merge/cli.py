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
import sys
from typing import Any, Dict

from .core import MergeError, Replica
from .protocol import ProtocolError, load_handoff_file


def _emit(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


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
    except (ProtocolError, MergeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
