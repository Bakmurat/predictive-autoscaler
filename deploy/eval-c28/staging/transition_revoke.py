#!/usr/bin/env python3
"""The ordinary revocation of the operator's replica write, and the cluster readers both revocations use
(TRANSITION-DESIGN draft 6, sections 2 and 3, with Amendment 1).

`revoke` is a reviewed operation under the shared evaluation lock: plan -> digest -> apply. Unlike the emergency it
needs the complete transition identity, a verified drain interval at plan time, no open emergency and NO other
outstanding executor: holding the lock does not prove that an earlier executor or its child is gone, so that is the
emergency's or the recovery's to establish, never assumed here. Every deletion goes through the journal.

Order of `apply`, all of it under the lock:
  1 the plan is computed again and must be the reviewed one (so: the same binding, API servers, emergency sequence,
    chart obligations; no emergency open; nobody else outstanding);
  2 this process registers as an executor (its own process group);
  3 the chart-restoration obligation is made durable BEFORE the first deletion (the chart still grants);
  4 the binding is deleted by its observed UID (another UID than the planned one is recorded as drift and deleted too);
  5 a full verified interval of confirmed absence (the same loop as the emergency, deletions journalled);
  6 the targets' /scale read twice, two decision intervals apart (an OBSERVATION: it proves nothing and closes
    nothing), and the binding is LOOKED AT throughout that wait: seen again, it is deleted and nothing is claimed;
  7 LAST, the permission proof, checked as strictly as the emergency checks it;
  8 the result is recorded; the registration is finished.
The lock is released only for a proven, recorded revocation. Any other end keeps it (replica-write permission may
still exist, so no other helper may run), and the generic recovery does not release it either
(transition_records.release_problem): the way on is the emergency.

What it does NOT do, and says so in its result: it does not restore the chart to its shadow values (the obligation
stays pending and blocks grant and activate; no Helm runner exists yet), and it does not read scale_writes_total.
"Permission revoked" is therefore not a completed rollback, and it permits nothing: it closes no activation
obligation and is no reason to remove the watchdog.
"""
import hashlib
import json
import math
import subprocess
import time

import common_records as C
import execlib as X
import permissions as P
import transition_drain as D
import transition_emergency as E
import transition_fence as F
import transition_records as R

APISERVER_SCHEMA = "c28-transition-apiserver/v1"
CONTROL_PLANE_LABEL = "node-role.kubernetes.io/control-plane"
APISERVER_NAMESPACE, APISERVER_COMPONENT = "kube-system", "kube-apiserver"
SCALE_GAP_DECISION_INTERVALS = 2
LEFT_PENDING = ["the chart restore to its shadow values", "the scale_writes_total corroboration"]


# --- readers (used by the emergency and by revoke) ---------------------------------------------------------------------

def apiserver_reference(record):
    """The reviewed control-plane inventory and default attestations the record names (transition.apiserver: path and
    sha256), checked against that hash on the very bytes that are parsed. {"sha256", "nodes", "defaults"}."""
    section = record.doc.get("transition")
    ref = section.get("apiserver") if isinstance(section, dict) else None
    if not isinstance(ref, dict) or not isinstance(ref.get("path"), str) or not ref["path"] \
            or not isinstance(ref.get("sha256"), str):
        raise X.StagingError("the record names no reviewed API-server reference (transition.apiserver: path, sha256)")
    try:
        with open(record.resolve(ref["path"]), "rb") as fh:
            data = fh.read()
    except OSError as e:
        raise X.StagingError(f"the API-server reference cannot be read ({type(e).__name__})") from e
    if hashlib.sha256(data).hexdigest() != ref["sha256"]:
        raise X.StagingError("the API-server reference is not the reviewed file (sha256 differs)")
    try:
        doc = json.loads(data)
    except ValueError as e:
        raise X.StagingError("the API-server reference is not JSON") from e
    nodes = doc.get("control_plane_nodes") if isinstance(doc, dict) else None
    if not isinstance(doc, dict) or doc.get("schema") != APISERVER_SCHEMA or not isinstance(nodes, list) or not nodes \
            or not all(isinstance(n, dict) and set(n) == {"name", "uid"} and isinstance(n["name"], str) and n["name"]
                       and isinstance(n["uid"], str) and n["uid"] for n in nodes) \
            or len({n["name"] for n in nodes}) != len(nodes) or len({n["uid"] for n in nodes}) != len(nodes) \
            or not isinstance(doc.get("defaults"), dict):
        raise X.StagingError("the API-server reference does not hold a control-plane inventory (name and UID of each "
                             "node) and the default attestations")
    return {"sha256": ref["sha256"], "nodes": sorted(nodes, key=lambda n: n["name"]), "defaults": doc["defaults"]}


def _items(api, kind, namespace=None):
    doc = api.get(X.path(kind, namespace))
    items = doc.get("items") if isinstance(doc, dict) else None
    if not isinstance(items, list) or not all(isinstance(i, dict) for i in items):
        raise X.StagingError(f"the {kind} list was not answered as a list")
    return items


def _labels(obj):
    labels = (obj.get("metadata") or {}).get("labels") if isinstance(obj.get("metadata"), dict) else None
    return labels if isinstance(labels, dict) else {}


def drain_reader(record, api, clock=None):
    """A callable that reads the drain interval NOW. The expected API servers are the REVIEWED control-plane nodes
    (name and UID), never whatever happens to carry the label today: the labelled nodes must be exactly the reviewed
    ones, and transition_drain refuses a missing or an additional kube-apiserver pod. It raises when it cannot read."""
    def read():
        ref = apiserver_reference(record)
        C.require_cluster(api, record.cluster_uid)
        live = sorted(({"name": (n.get("metadata") or {}).get("name"), "uid": (n.get("metadata") or {}).get("uid")}
                       for n in _items(api, "Node") if CONTROL_PLANE_LABEL in _labels(n)),
                      key=lambda n: str(n["name"]))
        if live != ref["nodes"]:
            raise X.StagingError(f"the labelled control-plane nodes are not the reviewed ones (reviewed "
                                 f"{[n['name'] for n in ref['nodes']]}, labelled now {[n['name'] for n in live]}, "
                                 f"compared by name and UID)")
        pods = [p for p in _items(api, "Pod", APISERVER_NAMESPACE) if _labels(p).get("component") == APISERVER_COMPONENT]
        out = D.verdict(pods, ref["nodes"], ref["defaults"], C.utc_now(clock))
        out["reference"] = {"sha256": ref["sha256"]}
        return out
    return read


def proof_reader(record, identity, run=subprocess.run):
    """A callable that asks the permission proof for the operator NOW (permissions.py) and stores it durably BEFORE
    returning it: a proof that could not be stored is no proof (the callable raises)."""
    def read():
        if any(not identity.get(k) for k in ("release", "control_namespace", "target_namespace", "targets")):
            raise X.StagingError("the transition identity is incomplete: no permission proof can be asked for")
        k = record.doc["kubeconfig"]

        def kubectl(args):
            p = run(["kubectl", "--kubeconfig", k["path"], "--context", k["context"], "--request-timeout=10s", *args],
                    capture_output=True, text=True, timeout=E.CALL_TIMEOUT_SECONDS)
            return p.returncode, p.stdout or ""
        sa_ns, sa = identity["control_namespace"], f"{identity['release']}-predictive-autoscaler-operator"
        targets = list(identity["targets"])
        proof = P.verdict(P.collect(kubectl, sa_ns, sa, identity["target_namespace"], targets, "operator"),
                          sa_ns, sa, targets, record.cluster_uid)
        R.store_proof(record, proof)
        return proof
    return read


def scale_observation(api, namespace, targets, gap_seconds, sleep):
    """Design section 2, fact 3, as far as it is implemented: each target's /scale read twice, `gap_seconds` apart.
    An observation only. A read that fails is "unknown", never "unchanged"."""
    def read():
        seen = {}
        for name in targets:
            try:
                scale = api.get(X.path("Deployment", namespace, name) + "/scale")
                seen[name] = {"resourceVersion": scale["metadata"]["resourceVersion"],
                              "replicas": scale["spec"]["replicas"]}
            except Exception as e:                        # an observation: record that it failed, go on
                seen[name] = {"error": type(e).__name__}
        return seen
    first = read()
    sleep(gap_seconds)
    second = read()
    return {"first": first, "second": second, "gap_seconds": gap_seconds,
            "targets": {t: "unknown" if "error" in first[t] or "error" in second[t]
                        else "unchanged" if first[t] == second[t] else "changed" for t in targets},
            "scale_writes_total": "uncorroborated: not read",
            "note": "an observation, not a proof: it closes no obligation and permits nothing"}


# --- the ordinary revocation -------------------------------------------------------------------------------------------

class JournalledBinding:
    """The scale binding for the ordinary revocation: read like the emergency's ScaleBinding, deleted through the
    journal (intent before, outcome after, under the lock). After a deletion whose outcome is unknown it deletes
    nothing more: the journal holds an unresolved intent, which is the recovery's to decide."""

    def __init__(self, record, kube, release, target_namespace):
        self._plain = E.ScaleBinding(record, kube.api, release, target_namespace)
        self._kube = kube
        self.namespace, self.name = self._plain.namespace, self._plain.name
        self.unknown_outcome = False

    def observe(self):
        return self._plain.observe()

    def delete(self, uid):
        if self.unknown_outcome:
            raise X.StagingError("an earlier deletion's outcome is unknown: nothing more is deleted before a recovery")
        try:
            self._kube.delete("RoleBinding", self.name, self.namespace, uid)
        except C.ApiError as e:
            if getattr(e, "status", None) not in X.DEFINITIVE:
                self.unknown_outcome = True
            raise
        except BaseException:
            self.unknown_outcome = True
            raise


def plan(record, kube, drain, *, activation):
    """The revocation as the live state defines it. It refuses what an ordinary revocation cannot work with (an
    incomplete identity, records it cannot read, an unverified drain interval); what merely has to be resolved first
    (an open emergency, another outstanding executor) is listed under "problems", and apply refuses on any."""
    if not isinstance(activation, str) or not R.ACTIVATION.match(activation):
        raise X.StagingError("the activation id must be a short DNS label")
    identity, problems = R.identity_read(record)
    if problems:
        raise X.StagingError("an ordinary revocation needs the complete transition identity (" + "; ".join(problems)
                             + "); the emergency works with whatever is readable")
    emergencies, chart = R.emergency_state(record), R.chart_state(record)
    executors, log_problem = R.outstanding(record)
    for what, problem in (("emergency", emergencies["problem"]), ("chart", chart["problem"]),
                          ("executor", log_problem)):
        if problem:
            raise X.StagingError(f"the {what} records cannot be read with certainty ({problem})")
    binding = E.ScaleBinding(record, kube.api, identity["release"], identity["target_namespace"])
    state, uid = binding.observe()
    reading = drain()
    if E._interval(reading) is None:
        raise X.StagingError(f"the drain interval is not verified ({reading.get('why') if isinstance(reading, dict) else reading!r}"
                             f"): an ordinary revocation could prove nothing; the emergency removes the permission "
                             f"without it")
    found = []
    if emergencies["open"]:
        found.append(f"emergencies {emergencies['open']} are open: complete them first")
    if executors:
        found.append(f"executors {[e['id'] for e in executors]} are outstanding: the lock does not prove they are "
                     f"gone; the emergency or the recovery establishes their fence")
    actions = [{"op": "delete", "kind": "RoleBinding", "namespace": binding.namespace, "name": binding.name,
                "uid": uid}] if state == "present" else []
    return X.new_plan(R.HELPER, record, actions, operation="revoke", activation=activation, identity=identity,
                      binding={"namespace": binding.namespace, "name": binding.name, "state": state, "uid": uid},
                      drain={k: v for k, v in reading.items() if k != "observed_at"},
                      scale_gap_seconds=SCALE_GAP_DECISION_INTERVALS * identity["decision_interval_seconds"],
                      scale_gap_source=f"{SCALE_GAP_DECISION_INTERVALS} x {identity['cadence_source']}",
                      emergency_last=emergencies["last"], chart_pending=chart["pending"],
                      leaves_pending=list(LEFT_PENDING), problems=found, **_image_note(record))


def _image_note(record):
    """An open image transition is a NOTE in a revocation's plans, never a problem: removing the operator's write
    permission is not fenced by it (execlib.IMAGE_FENCE_EXEMPT); the image fence itself stays as it is."""
    fence = X.image_fence(record)
    return {"image_transition": {"open": fence, "note": "not fenced: a revocation removes a permission and touches "
                                                        "no Deployment; the image fence stays in place"}} if fence \
        else {}


def apply(record, kube, reviewed, reviewed_digest, *, drain, permissions, mono=time.monotonic, sleep=time.sleep,
          clock=None, own_group=F.own_group, started=R.started, finished=R.finished):
    """Carry out the reviewed revocation. Returns the result; status is "permission-revoked" only when the binding
    was observed absent for a full verified interval and until the end, the proof (asked LAST) holds, and the
    result was recorded. Every other end keeps the shared lock."""
    if not isinstance(reviewed, dict) or reviewed.get("operation") != "revoke":
        raise X.StagingError("this is not a revocation plan")
    problems, deletions, late = [], [], []
    with X.mutating(record, kube, R.HELPER, {"operation": "revoke"}) as lock:
        # Under the lock: the state the review saw must be the state NOW (an emergency that came and went, another
        # executor, a replaced binding or API server all change the plan).
        live = plan(record, kube, drain, activation=reviewed.get("activation"))
        X.require_live(reviewed, reviewed_digest, R.HELPER, record, live)
        if live["problems"]:
            raise X.StagingError("the revocation cannot start: " + "; ".join(live["problems"]))
        identity, activation = live["identity"], live["activation"]
        own_group()
        # From the registration on, the lock is released ONLY by one of two explicit decisions below: a proven,
        # recorded revocation, or a fully recorded "never started" with the registration finished. Everything else
        # (a registration that half happened, a cleanup that failed, any exception) keeps it.
        release = False
        try:
            executor = started(record, "revoke", activation, lock, clock,
                               {"protocol": R.PROTOCOL, "emergency_last": live["emergency_last"]})
            try:
                chart = R.chart_pending(record, f"revocation {executor}", clock)   # durable BEFORE any deletion
            except BaseException:
                X.receipt(record, R.HELPER, "revocation", {"executor": executor, "outcome": "not-started"}, clock)
                finished(record, executor, lock, children_collected=True, clock=clock)
                release = True                            # recorded as never started, and finished: nothing was done
                raise
            binding = JournalledBinding(record, kube, identity["release"], identity["target_namespace"])
            t0, started_at = mono(), C.utc_now(clock)
            E._remove(binding, problems, deletions)
            absent, drained, drain_evidence = E.confirm_absence(binding, drain, mono(), t0, problems=problems,
                                                                deletions=deletions, mono=mono, sleep=sleep)
            as_planned = D.same_servers(reviewed.get("drain"), drain_evidence["first_reading"])
            if not as_planned:
                problems.append("the API servers are not the ones the plan was reviewed with")

            def watch(seconds):
                """The wait between the two /scale reads, spent looking at the binding. Seen again, it is deleted (it
                grants) and the revocation claims nothing: the interval that elapsed was not the last word."""
                end = mono() + seconds
                while True:
                    seen = E._attempt(problems, "reading the scale binding", binding.observe)
                    late.append([round(mono() - t0, 3), "unknown" if seen is None else seen[0]])
                    if seen is not None and seen[0] == "present":
                        E._remove(binding, problems, deletions)
                    remaining = end - mono()
                    if remaining <= 0:
                        return
                    sleep(min(E.POLL_SECONDS, remaining))
            observed = scale_observation(kube.api, identity["target_namespace"], identity["targets"],
                                         live["scale_gap_seconds"], watch)
            stayed_absent = bool(late) and all(state == "absent" for _at, state in late)
            if not stayed_absent:
                problems.append("the scale binding was not seen absent throughout the observation after the interval")
            if binding.unknown_outcome:
                problems.append("a deletion's outcome is unknown: the journal holds an unresolved intent (recover)")
            proof = E._attempt(problems, "the permission proof", permissions)           # LAST: after every look
            why = E.proof_problems(proof, record, identity["operator_identity"], identity["targets"])
            problems.extend(f"the permission proof: {w}" for w in why)
            holds = absent and drained["verified"] and drained["elapsed"] and drained["unchanged"] and as_planned \
                and stayed_absent and not why and not binding.unknown_outcome
            planned_uid = live["binding"]["uid"]
            facts = {
                "outcome": "revoked" if holds else "not-revoked", "executor": executor, "activation": activation,
                "binding_removed": absent and stayed_absent, "writes_denied": not why,
                "binding": f"{binding.namespace}/{binding.name}", "planned_uid": planned_uid,
                "deletions": list(deletions), "drift": [u for u in deletions if u != planned_uid],
                "started_at": started_at, "finished_at": C.utc_now(clock),
                "drain": dict(drain_evidence, as_planned=as_planned),
                "after_the_interval": {"stayed_absent": stayed_absent, "observations": late[-E.MAX_OBSERVATIONS_KEPT:],
                                       "observations_total": len(late)},
                "permissions": {"sha256": C.digest(proof) if isinstance(proof, dict) else None,
                                "verdict": proof.get("verdict") if isinstance(proof, dict) else None,
                                "expected_identity": identity["operator_identity"],
                                "expected_targets": identity["targets"], "problems": why},
                "scale_observation": observed, "chart": {"state": "restore-pending", "seq": chart},
                "not_done": list(LEFT_PENDING), "problems": list(problems)[:50],
            }
            recorded = E._attempt(problems, "recording the result",
                                  lambda: X.receipt(record, R.HELPER, "revocation", facts, clock)) is not None
            finished(record, executor, lock, children_collected=True, clock=clock)   # no further action; NOT "it worked"
            release = holds and recorded
        finally:
            lock.safe = bool(release)                     # the journal's own flag is not the last word here
    return {"status": "permission-revoked" if holds and recorded else "unresolved", "recorded": recorded,
            "lock_kept": not (holds and recorded), "evidence": facts, "problems": problems}


# --- the transition's recovery -----------------------------------------------------------------------------------------

def recovery_part(record, lock_sha256):
    """What a recovery has to do for the transition, from the records alone: which executors it must fence, how each
    unsettled revocation can be settled (or why it cannot), and whether releasing THIS stale lock rests on the
    revoked permission (then the releasing recovery must see it itself, also when it resumes an interrupted one).
    Deterministic: the plan's digest covers it, and it is computed again inside the entered recovery guard."""
    summary, problem = R.owner_summary(record)
    found, problem2 = R.revocations(record)
    emergencies, chart = R.emergency_state(record), R.chart_state(record)
    for what in (problem, problem2, emergencies["problem"], chart["problem"]):
        if what:
            raise X.StagingError(f"the transition's records cannot be read with certainty ({what})")
    settle, problems = [], []
    if emergencies["open"]:
        problems.append(f"emergencies {emergencies['open']} are open: complete them first")
    for r in found:
        if r["outcome"] in R.SETTLED_OUTCOMES or (r["settlement"] is not None and R.settlement_problem(
                record, r, r["settlement"], summary["completed"]) is None):
            continue
        if r["protocol"] != R.PROTOCOL:
            problems.append(f"{r['executor']}: its registration is not of the current protocol; it stays unresolved")
        elif not r["intents"] and r["outcome"] is None:
            settle.append({"executor": r["executor"], "by": "not-started"})
        else:
            later = [seq for seq in emergencies["tombstones"] if r["emergency_last"] is not None
                     and seq > r["emergency_last"]]
            covering = [(seq, sha) for seq, (doc, sha) in ((s, R._tombstone(record, s)) for s in later)
                        if doc is not None and R.covers(doc, r["executor"])]
            if covering:
                seq, sha = covering[-1]
                settle.append({"executor": r["executor"], "by": "tombstone", "tombstone": seq,
                               "tombstone_sha256": sha})
            else:
                problems.append(f"{r['executor']}: no emergency later than sequence {r['emergency_last']} fenced it "
                                f"or found it completed: run the emergency first")
    needing, problem = R.release_needs_proof(record, lock_sha256) if lock_sha256 else ([], None)
    if problem:
        raise X.StagingError(f"the transition's records cannot be read with certainty ({problem})")
    return {"fence": [e["id"] for e in summary["outstanding"]], "settle": settle,
            "release_needs_proof": bool(needing) or any(s["by"] == "tombstone" for s in settle),
            "emergency": {"last": emergencies["last"], "open": emergencies["open"],
                          "tombstones": emergencies["tombstones"]},
            "chart_pending": chart["pending"], "problems": problems}


def recover_plan(record, kube):
    """The generic recovery plan (the held lock, every unresolved journal intent against the live state) and the
    transition's part. This recovery fences, settles and releases the lock; it deletes nothing, writes no
    PredictiveAutoscaler, runs no Helm, and closes no chart or activation obligation."""
    generic = X.recover_plan(record, kube)
    return X.new_plan(R.HELPER, record, generic["actions"], operation="recover", lock_sha256=generic["lock_sha256"],
                      generic_digest=X.digest(generic), unresolved=generic["unresolved"],
                      transition=recovery_part(record, generic["lock_sha256"]), **_image_note(record))


def recover_apply(record, kube, reviewed, reviewed_digest, *, permissions, fence=F.fence, clock=None):
    """Carry out the reviewed recovery. Under the entered recovery guard, in this order: the transition's part is
    computed again and must be the reviewed one; every outstanding executor is fenced; if the release rests on the
    revoked permission (a revocation settled by a tombstone under this lock, now or by an earlier interrupted
    recovery), the binding must be seen absent and a fresh permission proof must hold (stored before use), every
    time; the settlements are recorded; the guard is told what this recovery saw, and the release gate
    (transition_records.release_problem) checks it once more. Any failure keeps the lock. An interrupted recovery
    is resumed with a new, reviewed plan: what was already recorded no longer appears in it, the fresh checks do."""
    if not isinstance(reviewed, dict) or reviewed.get("operation") != "recover":
        raise X.StagingError("this is not a recovery plan")
    live = recover_plan(record, kube)
    X.require_live(reviewed, reviewed_digest, R.HELPER, record, live)
    if live["transition"]["problems"]:
        raise X.StagingError("the recovery cannot settle everything: " + "; ".join(live["transition"]["problems"]))
    generic = X.recover_plan(record, kube)
    if X.digest(generic) != live["generic_digest"]:
        raise X.StagingError("the journal's state changed while the recovery was starting; plan and review again")
    done = {"fenced": [], "settled": []}

    def follow(_actions, guard):
        part = recovery_part(record, live["lock_sha256"])  # INSIDE the guard, before any fence or settlement
        if C.digest(part) != C.digest(live["transition"]):
            raise X.StagingError("the transition's records changed since the review; plan and review again")
        for executor in part["fence"]:
            R.fenced(record, executor, guard, fence=fence, clock=clock)
            done["fenced"].append(executor)
        proof_digest = None
        if part["release_needs_proof"]:
            identity, problems = R.identity_read(record)
            if problems:
                raise X.StagingError("the transition identity is incomplete: " + "; ".join(problems))
            state, _uid = E.ScaleBinding(record, kube.api, identity["release"],
                                         identity["target_namespace"]).observe()
            if state != "absent":
                raise X.StagingError("the scale binding exists: a recovery deletes nothing; run the emergency")
            proof = permissions()                         # stored durably before it is returned
            why = R.proof_problems(proof, record, identity["operator_identity"], identity["targets"])
            if why:
                raise X.StagingError("the permission proof does not hold: " + "; ".join(why))
            proof_digest = C.digest(proof)
        for s in part["settle"]:
            settlement = dict(s) if s["by"] == "not-started" else dict(s, binding="absent",
                                                                       proof_sha256=proof_digest)
            R.settle(record, guard, settlement, clock)
            done["settled"].append(s["executor"])
        if proof_digest is not None:
            guard.fresh_revocation = {"binding": "absent", "proof_sha256": proof_digest}
    receipt = X.recover(record, kube, generic, X.digest(generic), guard_factory=R.recovery,
                        follow_up_guarded=follow, revocation=True)
    return {"status": "lock-recovered", "fenced": done["fenced"], "settled": done["settled"],
            "receipt": receipt.get("event") if isinstance(receipt, dict) else None,
            "not_done": ["the PredictiveAutoscalers are untouched", "the chart restore to its shadow values"]}
