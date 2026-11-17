#!/usr/bin/env python3
"""Emergency revocation of the operator's replica write (TRANSITION-DESIGN draft 6, section 3, with Amendment 1).

It removes ONE object, the RoleBinding <release>-predictive-autoscaler-operator-scale in the target namespace (the
chart's grant of `update` on deployments/scale), and then establishes, fact by fact, whether "revoked" may be claimed.

It does NOT take the shared evaluation lock and does not use the journal: it must work while a hung executor holds
that lock. That bypass is confined: `ScaleBinding` can read and delete exactly that one object and nothing else; it
checks the cluster identity before every deletion and deletes only by the UID it just observed. It never touches a
PredictiveAutoscaler: deactivation is done afterwards by the reviewed recover/revoke, through the journal.

The steps fail independently. A step that fails is recorded and the next one still runs:

  0 inputs    the binding is named from the release and the target namespace. If they are not known, nothing can be
              deleted or observed; that is said, the executors are fenced all the same, and the result is never
              "revoked". An unknown operator identity or target list never stops a step: the proof then cannot hold.
  1 request   record the emergency (transition_records), or, with `complete=<seq>`, take up an emergency that is
              still open (a new sequence never closes an earlier one: each open emergency needs its own tombstone).
              If this fails, everything below still happens; the result can then never be "revoked".
  2 delete    read the binding; delete it by its observed UID; read again. At most DELETE_ATTEMPTS rounds. This step
              states nothing about absence.
  3 fence     fence EVERY outstanding transition executor (transition_fence). A log that cannot be read is unproven.
  4 drain     look at the binding every POLL_SECONDS. The drain interval (transition_drain, verified) counts from
              CONFIRMED ABSENCE: the first of an unbroken run of observations that saw the binding absent, and no
              earlier than the end of the fence. An observation that sees the binding, or that fails, ends the run:
              whatever was counted is void and a full interval starts at the next confirmed absence. This does not
              depend on whether a DELETE answered: a deletion whose answer was lost, or a removal by someone else,
              restarts the interval just the same. More than MAX_REAPPEARANCES sightings, or no completed interval
              within the overall bound, is unresolved. At the end the API servers must still be the ones the interval
              was read from. Without a verified, finite interval there is one more look and no claim.
  5 proof     the permission proof for the operator's identity (permissions.py): verdict "pass" with `refused` and
              `unproven` present and empty, for the recorded cluster, the expected identity, role and targets.
  6 outcome   "revoked" ONLY if: the request was recorded (or the open sequence taken up), the last observation saw
              absence, every executor is fenced, a verified drain interval elapsed with unchanged API servers, the
              proof holds, AND the tombstone itself was written. The tombstone carries the evidence behind each
              statement (both drain readings, the required and the actual interval, the observations, the fence
              results with the owners' identities, the proof's hash). Otherwise "fence-unproven" or "not-revoked" is
              recorded and the emergency stays open.

Nothing here is a claim until `run()` returns status "revoked". Every uncertainty lowers the result; none raises it.
"""
import math
import subprocess
import time

import common_records as C
import execlib as X
import transition_drain as D
import transition_fence as F
import transition_records as R

DELETE_ATTEMPTS = 3
MAX_REAPPEARANCES = 3
POLL_SECONDS = 5.0
CALL_TIMEOUT_SECONDS = 20          # a real timeout for each kubectl subprocess (the shared wrapper sets none)
MAX_OBSERVATIONS_KEPT = 400


def binding_name(release):
    return f"{release}-predictive-autoscaler-operator-scale"


def operator_identity(release, control_namespace):
    """The user name of the operator's ServiceAccount, as permissions.py reports it."""
    return R.operator_identity(release, control_namespace)


def bounded_api(record, timeout=CALL_TIMEOUT_SECONDS):
    """The shared kubectl wrapper with a REAL subprocess timeout on every call (it passes kubectl's own
    --request-timeout but does not bound the subprocess)."""
    def run(args, **kw):
        return subprocess.run(args, timeout=timeout, **kw)
    return C.KubectlApi(record.doc["kubeconfig"], run=run, timeout="10s")


class ScaleBinding:
    """The one object emergency may read and delete. The path is fixed at construction from the release and the
    target namespace; no method takes a kind, a namespace, a name or a path."""

    def __init__(self, record, api, release, target_namespace):
        for value, what in ((release, "release"), (target_namespace, "target namespace")):
            if not isinstance(value, str) or not value or "/" in value or value != value.strip():
                raise X.StagingError(f"emergency needs the {what}")
        self._record, self._api = record, api
        self.namespace, self.name = target_namespace, binding_name(release)
        self._path = X.path("RoleBinding", self.namespace, self.name)

    def observe(self):
        """("absent", None) only when the API answered that there is no such object; ("present", uid) with a
        non-empty UID; raises for anything else (an error, an object without a UID, an object of another name)."""
        obj = self._api.get(self._path)
        if obj is None:
            return "absent", None
        meta = obj.get("metadata") if isinstance(obj, dict) else None
        uid = meta.get("uid") if isinstance(meta, dict) else None
        if not isinstance(meta, dict) or meta.get("name") != self.name or meta.get("namespace") != self.namespace \
                or not isinstance(uid, str) or not uid:
            raise X.StagingError("the scale binding was read, but not as an identifiable object")
        return "present", uid

    def delete(self, uid):
        """Delete by the observed UID, after checking that the kubeconfig still reaches the recorded cluster."""
        if not isinstance(uid, str) or not uid:
            raise X.StagingError("the scale binding is deleted only by an observed UID")
        C.require_cluster(self._api, self._record.cluster_uid)
        self._api.delete(self._path, uid)


def _attempt(problems, what, fn):
    """Run one step; an exception of any kind is recorded as a problem and returned as None."""
    try:
        return fn()
    except Exception as e:                                # every step is independent: record, go on
        problems.append(f"{what}: {type(e).__name__}: {str(e)[:200]}")
        return None


def _remove(binding, problems, deletions):
    """Up to DELETE_ATTEMPTS rounds of observe / delete. It returns nothing and states nothing: neither that the
    binding is gone nor when. Absence, and the time the drain counts from, come only from later observations."""
    for _ in range(DELETE_ATTEMPTS):
        seen = _attempt(problems, "reading the scale binding", binding.observe)
        if seen is None:
            continue                                      # unknown: neither absent nor deletable
        if seen[0] == "absent":
            return
        try:
            binding.delete(seen[1])
            deletions.append(seen[1])
        except C.NotFound:
            pass                                          # gone between the read and the delete: not our deletion
        except Exception as e:                            # a lost answer included: the delete may still have applied
            problems.append(f"deleting the scale binding: {type(e).__name__}: {str(e)[:200]}")


def proof_problems(proof, record, identity, targets):
    """Why this permission proof does not establish that the operator's scale write is denied. Empty = it does.
    Every field must be PRESENT and of the expected type: a missing list is not an empty list."""
    if not isinstance(proof, dict):
        return ["the permission proof is not a result"]
    why = []
    if proof.get("verdict") != "pass":
        why.append(f"the verdict is {proof.get('verdict')!r}, not 'pass'")
    for key in ("refused", "unproven"):
        if key not in proof or not isinstance(proof[key], list):
            why.append(f"the proof does not carry its {key!r} list")
        elif proof[key]:
            why.append(f"the proof lists {len(proof[key])} {key} finding(s)")
    if proof.get("cluster_uid") != record.cluster_uid or proof.get("recorded_cluster_uid") != record.cluster_uid:
        why.append("the proof is not of the recorded cluster")
    if proof.get("identity") != identity or proof.get("role") != "operator":
        why.append("the proof is not about the operator's identity")
    asked = proof.get("targets")
    if not isinstance(asked, list) or not asked or sorted(map(str, asked)) != sorted(targets):
        why.append("the proof is not about the expected targets")
    return why


def _interval(verdict):
    """The verified drain interval in seconds, or None."""
    if not isinstance(verdict, dict) or verdict.get("verified") is not True:
        return None
    seconds = verdict.get("seconds")
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0:
        return None
    return float(seconds)


def settle(binding, seconds, fence_done, *, problems, deletions, observations, offset, mono, sleep):
    """Look at the binding every POLL_SECONDS until a full interval of CONFIRMED absence has elapsed (module text,
    step 4), deleting it again when it is seen. `binding`: anything with observe() and delete(uid) of a ScaleBinding
    (the ordinary revocation passes one whose deletions go through the journal). `seconds` None = no verified
    interval: one more look, a poll later, and no claim.
    Returns {"absent", "absent_since", "reappearances", "elapsed", "elapsed_seconds"}."""
    give_up = mono() + (MAX_REAPPEARANCES + 2) * ((seconds or POLL_SECONDS) + POLL_SECONDS)
    absent_since, reappearances = None, 0
    wait = 0.0 if seconds is not None else POLL_SECONDS   # unverified: the one more look comes a poll later
    while True:
        sleep(wait)
        seen = _attempt(problems, "reading the scale binding", binding.observe)
        now = mono()
        if seen is None:                                  # unknown: not absent, and it ends a run of absence
            observations.append([offset(), "unknown"])
            absent_since = None
        elif seen[0] == "absent":
            observations.append([offset(), "absent"])
            if absent_since is None:
                absent_since = now                        # confirmed absence starts HERE, at an observation
        else:                                             # it is there: whatever was counted is void
            observations.append([offset(), "present"])
            absent_since = None
            reappearances += 1
            if reappearances > MAX_REAPPEARANCES:
                problems.append("the scale binding kept reappearing: unresolved")
                break
            _remove(binding, problems, deletions)
        if seconds is None:
            break                                         # no verified interval: this was the one more look
        counted_from = max(absent_since, fence_done) if absent_since is not None else None
        if counted_from is not None and now - counted_from >= seconds:
            break                                         # a full interval of confirmed absence
        if now >= give_up:
            problems.append("no full interval of confirmed absence within the bound: unresolved")
            break
        wait = POLL_SECONDS if counted_from is None else max(0.0, min(POLL_SECONDS, counted_from + seconds - now))
    end = mono()
    absent = absent_since is not None and reappearances <= MAX_REAPPEARANCES
    counted_from = max(absent_since, fence_done) if absent_since is not None else None
    elapsed_seconds = end - counted_from if counted_from is not None else None
    return {"absent": absent, "absent_since": absent_since, "reappearances": reappearances,
            "elapsed_seconds": None if elapsed_seconds is None else round(elapsed_seconds, 3),
            "elapsed": seconds is not None and absent and elapsed_seconds is not None and elapsed_seconds >= seconds}


def confirm_absence(binding, drain, fence_done, t0, *, problems, deletions, mono, sleep):
    """Step 4 as a whole, for emergency and for the ordinary revocation: read the drain interval, settle, read it
    again. Returns (absent, state, evidence): `state` is the short summary, `evidence` what a record keeps."""
    observations = []
    first = _attempt(problems, "reading the drain interval", drain)
    seconds = _interval(first)
    if seconds is None:
        problems.append("the drain interval is not verified: "
                        + str(first.get("why") if isinstance(first, dict) and first.get("why") else "no usable reading"))
    got = settle(binding, seconds, fence_done, problems=problems, deletions=deletions, observations=observations,
                 offset=lambda: round(mono() - t0, 3), mono=mono, sleep=sleep)
    last, unchanged = None, False
    if seconds is not None:
        last = _attempt(problems, "reading the drain interval again", drain)
        unchanged = D.same_servers(first, last)
        if not unchanged:
            problems.append("the API servers are not the ones the drain interval was read from")
    state = {"verified": seconds is not None, "seconds": seconds, "elapsed": got["elapsed"],
             "elapsed_seconds": got["elapsed_seconds"], "unchanged": unchanged, "reappearances": got["reappearances"]}
    evidence = {"verified": state["verified"], "required_seconds": seconds, "elapsed_seconds": got["elapsed_seconds"],
                "elapsed": got["elapsed"], "unchanged": unchanged, "reappearances": got["reappearances"],
                "confirmed_absent_at_offset": None if got["absent_since"] is None else round(got["absent_since"] - t0, 3),
                "first_reading": first if isinstance(first, dict) else None,
                "last_reading": last if isinstance(last, dict) else None,
                "observations": observations[-MAX_OBSERVATIONS_KEPT:], "observations_total": len(observations)}
    return got["absent"], state, evidence


NO_DRAIN = {"verified": False, "seconds": None, "elapsed": False, "elapsed_seconds": None, "unchanged": False,
            "reappearances": 0}


def run(record, binding, *, reason, drain, permissions, identity, targets, complete=None, fence=F.fence,
        mono=time.monotonic, sleep=time.sleep, clock=None):
    """Do the emergency. `binding`: a ScaleBinding, or None when it cannot be named; `drain()`: a transition_drain
    verdict read NOW (may raise); `permissions()`: a permissions.verdict read NOW (may raise); `identity`, `targets`:
    whom and what the proof must be about (None when unknown: the proof then cannot hold); `complete`: the sequence
    number of an OPEN emergency to take up instead of opening a new one.
    Returns the result; status is "revoked" only when every fact holds and the tombstone was written."""
    if binding is not None and not isinstance(binding, ScaleBinding):
        raise X.StagingError("emergency acts on a ScaleBinding and on nothing else")
    problems, deletions = [], []
    known = isinstance(identity, str) and bool(identity) and isinstance(targets, (list, tuple)) and bool(targets) \
        and all(isinstance(t, str) and t for t in targets)
    targets = sorted(targets) if known else None
    if not known:
        problems.append("the operator's identity or the targets are not known: the permission proof cannot hold")
    if binding is None:
        problems.append("the scale binding cannot be named (release or target namespace unknown): nothing is deleted")
    t0, started_at = mono(), C.utc_now(clock)
    out = {"status": "unresolved", "seq": None, "request_recorded": False, "completing": complete,
           "binding": None if binding is None else f"{binding.namespace}/{binding.name}", "deletions": deletions,
           "absent": False, "fence": {"proven": False, "executors": [], "problem": None}, "drain": dict(NO_DRAIN),
           "writes_denied": False, "permissions": None, "outcome": None, "outcome_recorded": False,
           "open_emergencies": None, "problems": problems}

    # 1 request, or an open emergency taken up
    if complete is None:
        seq = _attempt(problems, "recording the emergency request", lambda: R.emergency_request(record, reason, clock))
    else:
        state = _attempt(problems, "reading the emergency records", lambda: R.emergency_state(record))
        seq = complete if isinstance(state, dict) and not state["problem"] and not isinstance(complete, bool) \
            and isinstance(complete, int) and complete in state["open"] else None
        if seq is None:
            problems.append(f"emergency {complete!r} is not an open emergency that can be completed")
    out["seq"], out["request_recorded"] = seq, seq is not None

    # 2 delete (never waits for anything above or below; states nothing)
    if binding is not None:
        _remove(binding, problems, deletions)

    # 3 fence every outstanding executor, whatever happened so far
    listed = _attempt(problems, "reading the outstanding executors", lambda: R.outstanding(record))
    executors, log_problem = listed if listed is not None else ([], "the outstanding executors could not be read")
    out["fence"]["problem"] = log_problem
    proven = log_problem is None
    for e in executors:
        result = _attempt(problems, f"fencing {e.get('id')}", lambda e=e: fence(dict(e["owner"])))
        ok = isinstance(result, dict) and result.get("proven") is True
        out["fence"]["executors"].append({"id": e.get("id"), "owner": e.get("owner"), "proven": ok,
                                          "result": result if isinstance(result, dict) else None})
        proven = proven and ok
    out["fence"]["proven"] = proven
    fence_done, fence_done_offset = mono(), round(mono() - t0, 3)

    # 4 drain: a full verified interval of CONFIRMED absence, not earlier than the fence
    drain_evidence = None
    if binding is not None:
        out["absent"], out["drain"], drain_evidence = confirm_absence(
            binding, drain, fence_done, t0, problems=problems, deletions=deletions, mono=mono, sleep=sleep)

    # 5 proof
    proof = _attempt(problems, "the permission proof", permissions)
    why = proof_problems(proof, record, identity, targets) if known \
        else ["the expected identity and targets are not known"]
    out["permissions"] = proof.get("verdict") if isinstance(proof, dict) else None
    out["writes_denied"] = not why
    problems.extend(f"the permission proof: {w}" for w in why)

    # 6 outcome, with the evidence behind every statement
    all_hold = out["request_recorded"] and out["absent"] and out["fence"]["proven"] and out["writes_denied"] \
        and out["drain"]["verified"] and out["drain"]["elapsed"] and out["drain"]["unchanged"]
    out["outcome"] = "revoked" if all_hold else ("fence-unproven" if not out["fence"]["proven"] else "not-revoked")
    facts = {
        "binding_removed": out["absent"], "fence_proven": out["fence"]["proven"],
        "writes_denied": out["writes_denied"], "binding": out["binding"], "completing": complete,
        "started_at": started_at, "finished_at": C.utc_now(clock), "deletions": list(deletions),
        "fence": {"problem": log_problem, "completed_at_offset": fence_done_offset,
                  "executors": out["fence"]["executors"]},
        "drain": drain_evidence,
        "permissions": {"sha256": C.digest(proof) if isinstance(proof, dict) else None,
                        "verdict": out["permissions"], "expected_identity": identity if known else None,
                        "expected_targets": targets, "problems": why},
        "problems": list(problems)[:50],
    }
    out["evidence"] = facts
    if seq is not None:
        def write():
            try:
                R.emergency_close(record, seq, out["outcome"], facts, clock)
            except X.StagingError as e:
                # A sequence being completed may already carry an earlier failure record of the same kind: that
                # record stays as it is. A tombstone is never "already there" for an open emergency.
                if out["outcome"] != "revoked" and "already has" in str(e):
                    return "kept"
                raise
            return True
        written = _attempt(problems, "recording the outcome", write)
        out["outcome_recorded"] = written is True
    state = _attempt(problems, "reading the emergency records", lambda: R.emergency_state(record))
    out["open_emergencies"] = state["open"] if isinstance(state, dict) and not state["problem"] else None
    out["status"] = "revoked" if all_hold and out["outcome_recorded"] else "unresolved"
    return out
