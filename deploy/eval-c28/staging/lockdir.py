#!/usr/bin/env python3
"""Pre-flight view of the shared mutation-lock directory. READ-ONLY: it creates, writes, locks and removes nothing.

  lockdir.py --record R status        every entry of the record's lock_dir, classified; exit 0 when clear, 3 when not

Run it before any helper that takes the mutation lock, and read the whole answer:

  locks             a mutation lock exists: an execution is running, or it crashed (then: that helper's recover-plan)
  recovery_guards   a recovery is in progress or was interrupted (the next recovery takes a dead recoverer's guard over)
  coordination      <lock>.recovery.flock, the recoverers' flock file. EXPECTED and PERMANENT once a recovery has run:
                    it is NEVER deleted to make the directory look empty. A deleted file would let two recoverers each
                    hold "the" flock.
  other_clusters    entries of another cluster: named, not judged
  unexpected        anything else; it must be explained before a run

`clear` (exit 0) means: no lock, no recovery guard, nothing unexpected, no problem. It does NOT mean that a helper has
no open intent or obligation: ask each helper's own `status` for those.
"""

import argparse
import sys
from typing import List, Optional

import common_records as C
import execlib as X


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--record", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    a = ap.parse_args(argv)
    try:
        state = C.lock_dir_state(X.load_record(a.record, ()).doc)
    except C.StagingError as e:
        sys.stderr.write(f"lockdir: {e}\n")
        return 2
    X.emit(state)
    return 0 if state["clear"] else 3


if __name__ == "__main__":
    sys.exit(main())
