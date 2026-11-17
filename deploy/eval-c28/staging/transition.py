#!/usr/bin/env python3
"""The Active transition's commands (TRANSITION-DESIGN draft 6). So far: the identity, the emergency and revoke.

  transition.py --record R identity --targets a,b
      Write the transition identity once (release, namespaces, operator identity, targets, decision interval), from
      the demonstration stage's validated params; the targets given must be that stage's designated targets. It must
      exist before anything can grant: the emergency names the binding from it, not from the params.

  transition.py --record R emergency --reason TEXT [--complete SEQ]
      Remove the operator's replica write NOW. No plan, no digest, no lock, no precondition. --complete takes up an
      emergency that is still open instead of opening a new one.

  transition.py --record R revoke plan --activation ID
  transition.py --record R revoke apply --plan FILE --digest SHA256
      The ordinary, reviewed revocation under the shared lock.

  transition.py --record R recover plan
  transition.py --record R recover apply --plan FILE --digest SHA256
      The recovery of a stale shared lock with the transition's part: fence the executors that are neither finished
      nor fenced, settle the revocations that proved nothing (only against a later emergency that covers them, a
      binding seen absent and a fresh permission proof), then release the lock. It deletes nothing.

The FIRST output line states the result, the emergencies still open and the chart restorations still pending; a
state that could not be read is printed as UNKNOWN, never as an empty list. The JSON result follows.
Exit status: 0 only when THIS run's permission revocation was proven and recorded AND the chart obligation is on
record (or the identity or plan was written); 2 unresolved; 1 refused. Exit 0 never means the rollback is complete: see the first line.
"""
import argparse
import json
import subprocess
import sys

import common_records as C
import execlib as X
import transition_emergency as E
import transition_records as R
import transition_revoke as V


def _shown(value):
    return "UNKNOWN" if value is None else ("none" if not value else str(value))


def standing(record):
    """(open emergencies, pending chart restorations): each a list, or None when it cannot be known."""
    try:
        emergencies = R.emergency_state(record)
        open_ = None if emergencies["problem"] else emergencies["open"]
    except Exception:
        open_ = None
    try:
        pending = R.chart_state(record)["pending"]
    except Exception:
        pending = None
    return open_, pending


def first_line(what, record):
    open_, pending = standing(record)
    return f"{what}; open emergencies: {_shown(open_)}; chart restoration pending: {_shown(pending)}"


CADENCE_SOURCE = "params.demonstration.cadence.update_interval_seconds"


def identity(record, targets):
    """Write the transition identity from the record's params, loaded the way the demonstration stage loads them
    (the transition exists for that stage only). The targets given must be the stage's designated ones."""
    section = record.doc.get("prepare")
    stage = section.get("stage", "experiment") if isinstance(section, dict) else None
    if stage != "demonstration":
        raise X.StagingError(f"the Active transition is defined for the demonstration stage only (the record's "
                             f"prepare.stage is {stage!r})")
    params = X.load_params_stage(record, stage)
    designated = X.import_harness().demo.target_names(params)
    if sorted(targets) != sorted(designated):
        raise X.StagingError(f"the targets given {sorted(targets)} are not the stage's designated targets "
                             f"{sorted(designated)}")
    return R.identity_write(record, release=params["release"], control_namespace=params["control_namespace"],
                            target_namespace=params["namespace"], targets=designated,
                            decision_interval_seconds=params["demonstration"]["cadence"]["update_interval_seconds"],
                            cadence_source=CADENCE_SOURCE)


def emergency(record, reason, complete, api_factory, kubectl_run):
    identity, identity_problems = R.identity_read(record)
    api = api_factory(record)
    binding = None
    if identity["release"] and identity["target_namespace"]:
        binding = E.ScaleBinding(record, api, identity["release"], identity["target_namespace"])
    # The chart still grants after the binding is gone: make that obligation durable. Its failure stops nothing.
    try:
        chart = {"seq": R.chart_pending(record, f"emergency: {reason}"), "problem": None}
    except Exception as e:
        chart = {"seq": None, "problem": f"{type(e).__name__}: {str(e)[:200]}"}
    out = E.run(record, binding, reason=reason, drain=V.drain_reader(record, api),
                permissions=V.proof_reader(record, identity, kubectl_run), identity=identity["operator_identity"],
                targets=identity["targets"], complete=complete)
    out["identity_problems"], out["chart"] = identity_problems, chart
    out["not_done"] = ["the PredictiveAutoscalers are untouched (deactivate through recover or revoke)",
                       "the chart restore to its shadow values"]
    return out


def main(argv=None, api_factory=E.bounded_api, kubectl_run=subprocess.run) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--record", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    ident = sub.add_parser("identity")
    ident.add_argument("--targets", required=True, help="the target Deployment names, comma separated")
    em = sub.add_parser("emergency")
    em.add_argument("--reason", required=True)
    em.add_argument("--complete", type=int, default=None)
    rv = sub.add_parser("revoke").add_subparsers(dest="step", required=True)
    rp = rv.add_parser("plan")
    rp.add_argument("--activation", required=True)
    ra = rv.add_parser("apply")
    ra.add_argument("--plan", required=True)
    ra.add_argument("--digest", required=True)
    rc = sub.add_parser("recover").add_subparsers(dest="step", required=True)
    rc.add_parser("plan")
    rca = rc.add_parser("apply")
    rca.add_argument("--plan", required=True)
    rca.add_argument("--digest", required=True)
    a = ap.parse_args(argv)
    record = X.load_record(a.record)
    try:
        if a.cmd == "identity":
            doc = identity(record, [t for t in a.targets.split(",") if t])
            print(first_line("IDENTITY written", record))
            X.emit(doc)
            return 0
        if a.cmd == "emergency":
            out = emergency(record, a.reason, a.complete, api_factory, kubectl_run)
            revoked, obliged = out["status"] == "revoked", out["chart"]["seq"] is not None
            what = f"UNRESOLVED ({out['outcome']})" if not revoked else \
                "PERMISSION REVOKED (chart restoration pending; PredictiveAutoscalers untouched)" if obliged else \
                "PERMISSION REVOKED, BUT THE CHART OBLIGATION COULD NOT BE RECORDED (the chart still grants)"
            print(first_line(f"EMERGENCY {_shown(out['seq'])}: {what}", record))
            X.emit(out)
            return 0 if revoked and obliged else 2
        api = api_factory(record)
        kube = record.kube(api=api)
        if a.cmd == "recover":
            if a.step == "plan":
                saved = X.cli_plan(record, R.HELPER, V.recover_plan(record, kube), "recover-plan")
                saved["problems"] = X.load_plan(saved["plan"])["transition"]["problems"]
                print(first_line("RECOVER planned" + (" WITH PROBLEMS (apply will refuse)" if saved["problems"]
                                                      else ""), record))
                X.emit(saved)
                return 0
            fields, _ = R.identity_read(record)
            out = V.recover_apply(record, kube, X.load_plan(a.plan), a.digest,
                                  permissions=V.proof_reader(record, fields, kubectl_run))
            print(first_line("RECOVER: the shared lock is released (not a rollback: nothing was deleted, restored "
                             "or deactivated)", record))
            X.emit(out)
            return 0
        drain = V.drain_reader(record, api)
        if a.step == "plan":
            saved = X.cli_plan(record, R.HELPER, V.plan(record, kube, drain, activation=a.activation), "revoke-plan")
            print(first_line("REVOKE planned" + (" WITH PROBLEMS (apply will refuse)" if saved["problems"] else ""),
                             record))
            X.emit(saved)
            return 0
        fields, _ = R.identity_read(record)
        out = V.apply(record, kube, X.load_plan(a.plan), a.digest, drain=drain,
                      permissions=V.proof_reader(record, fields, kubectl_run))
        revoked = out["status"] == "permission-revoked"
        print(first_line("REVOKE: " + ("PERMISSION REVOKED, chart restoration pending (not a completed rollback)"
                                       if revoked else "UNRESOLVED, the shared lock is KEPT (next: emergency)"),
                         record))
        X.emit(out)
        return 0 if revoked else 2
    except C.StagingError as e:
        print(first_line(f"{a.cmd.upper()} REFUSED: {e}", record))
        return 1


if __name__ == "__main__":
    sys.exit(main())
