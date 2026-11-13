"""Shadow smoke checks S6 and S7 for the r3 product images (kind.sh --shadow), with an offline selftest.

  shadow_checks.py validated <live PA json> <ml-engine dir> <verdict out>   exit 0 pass, 1 not decided yet,
                                                                             2 fail, 3 not accepted yet (ask again)
  shadow_checks.py pods <pods json> <expected count> <component>           exit 0 pass, 2 fail (problems on stdout)
  shadow_checks.py selftest

S6 (validated): the live autoscaler resolves to the demonstration's validated profile with a selected lead of 600 s
(profile membership, checked offline on the live object) AND its request reached the forecasting service, which
answered for this autoscaler with a reason of missing history or data (Codex, review of PR #86). Proof that the
service answered: either an issued forecast (status.prediction.issuanceID) that is not yet trusted for a history,
calibration or shadow reason, or PredictionReady DataUnavailable whose message is the service's own 503 body
(mlapi_client.go: "ML API returned status 503: <body>") with error HistoryInsufficient, or DataUnavailable for data
that is too old (not "history unavailable": that is the service failing to read its metrics backend). A transport or
TLS failure, a refused request, the registration limit, or a configuration, settings or lead refusal is a failure.
A configuration, settings or lead refusal, or a live object outside the profile, fails at once (exit 2); any other
answer that is not accepted may be transient (a metrics outage, a forecaster restart) and is asked again until the
caller's deadline, which then fails with the last verdict (exit 3).

S7 (pods): exactly the expected number of the component's pods, every one Running with every container ready and
never restarted.
"""

from __future__ import annotations

import json
import re
import sys

SERVICE_TRUST_REASONS = ("HistoryInsufficient", "CalibrationInsufficient", "ShadowInsufficient")
SERVICE_503_REASONS = ("HistoryInsufficient", "DataUnavailable")
PERMANENT = ("ConfigurationNotValidated", "SettingsInvalid", "LeadInsufficient")
REFUSED = ("ConfigurationNotValidated", "SettingsInvalid", "LeadInsufficient", "RequestRefused",
           "AutoscalerLimitReached", "ForecastRejected", "IdentityMismatch", "StateUnavailable")
_BODY = re.compile(r"ML API returned status 503: (\{.*\})\s*$", re.S)


def service_answer(ready: dict, prediction: dict) -> tuple:
    """(answered, why): whether PredictionReady shows an answer of the forecasting service with an accepted reason."""
    reason, msg = ready.get("reason"), ready.get("message") or ""
    if reason in REFUSED:
        return False, f"refused: {reason}: {msg[:200]}"
    if reason in SERVICE_TRUST_REASONS:
        if not prediction.get("issuanceID"):
            return False, f"trust reason {reason} without an issued forecast"
        return True, f"the service issued {prediction['issuanceID']}; not trusted yet: {reason}"
    if reason == "DataUnavailable":
        m = _BODY.search(msg)
        if not m:
            return False, f"DataUnavailable without the service's 503 body (transport or TLS?): {msg[:200]}"
        try:
            detail = json.loads(m.group(1)).get("detail") or {}
            err, text = detail.get("error"), str(detail.get("message") or "")
        except (ValueError, AttributeError):
            return False, f"DataUnavailable with an unreadable body: {msg[:200]}"
        if err == "DataUnavailable" and text.startswith("history unavailable"):
            return False, f"the service could not read its metrics backend: {text[:200]}"
        if err in SERVICE_503_REASONS:
            return True, f"the service answered 503 {err}: {text[:120]}"
        return False, f"the service answered 503 {err}, not a missing-history or data reason"
    return False, f"PredictionReady {ready.get('status')}/{reason} is not an accepted answer: {msg[:200]}"


def validated(pa: dict, contracts) -> tuple:
    """(code, verdict): 0 pass, 1 not decided yet, 2 fail, 3 not accepted yet."""
    st = pa.get("status") or {}
    ready = next((x for x in st.get("conditions", []) if x.get("type") == "PredictionReady"), None)
    lead = (st.get("prediction") or {}).get("lead")
    if ready is None or not lead:
        return 1, {"problems": ["no PredictionReady or no selected lead yet"]}
    problems = []
    settings = contracts.resolve_settings(pa)
    sel, why = contracts.resolve_lead(pa, settings, (pa.get("metadata") or {}).get("generation"))
    if sel is None or sel.lead_seconds != 600:
        problems.append(f"selected lead {None if sel is None else sel.lead_seconds} ({why})")
    if contracts.validated_profile(settings, 600) is None:
        problems.append("the live object is not the validated profile")
    permanent = bool(problems) or ready.get("reason") in PERMANENT
    answered, how = service_answer(ready, st.get("prediction") or {})
    if not answered:
        problems.append(how)
    code = 0 if not problems else 2 if permanent else 3
    return code, {"reason": ready.get("reason"), "service": how, "lead": lead, "problems": problems}


def pods_problems(doc: dict, expected: int, component: str) -> list:
    items = (doc or {}).get("items") or []
    out = []
    if len(items) != expected:
        out.append(f"{component}: {len(items)} pods, {expected} expected")
    for p in items:
        name = (p.get("metadata") or {}).get("name")
        status = p.get("status") or {}
        if status.get("phase") != "Running":
            out.append(f"{name}: phase {status.get('phase')}")
        cs = status.get("containerStatuses") or []
        if not cs:
            out.append(f"{name}: no container status")
        for c in cs:
            if not c.get("ready") or c.get("restartCount") != 0:
                out.append(f"{name}/{c.get('name')}: ready {c.get('ready')}, restarts {c.get('restartCount')}")
    return out


def selftest() -> int:
    class Sel:
        def __init__(self, lead):
            self.lead_seconds = lead

    class Contracts:                      # the offline stand-in: profile membership is tested in ml-engine itself
        lead = 600

        @staticmethod
        def resolve_settings(pa):
            return pa

        @classmethod
        def resolve_lead(cls, pa, s, gen):
            return Sel(cls.lead), ""

        @staticmethod
        def validated_profile(s, lead):
            return object()

    def pa(reason, message="", status="False", issued=None):
        pred = {"lead": {"seconds": 600}, **({"issuanceID": issued} if issued else {})}
        return {"metadata": {"generation": 2}, "status": {"prediction": pred,
                "conditions": [{"type": "PredictionReady", "status": status, "reason": reason, "message": message}]}}
    body = 'ML API returned status 503: {"detail":{"error":"%s","message":"x"}}'
    ok = [pa("DataUnavailable", body % "HistoryInsufficient"),
          pa("DataUnavailable", body.replace('"x"', '"the newest finalized observation is too old"') % "DataUnavailable"),
          pa("CalibrationInsufficient", issued="iss-1"), pa("HistoryInsufficient", issued="iss-1")]
    bad = [pa("DataUnavailable", 'Post "https://c28-forecast-route:8443/v2/predict": tls: failed to verify certificate'),
           pa("RequestRefused", 'ML API refused the request (status 422): {"detail":{"error":"IdentityMismatch"}}'),
           pa("AutoscalerLimitReached", body % "AutoscalerLimitReached"),
           pa("DataUnavailable", body % "StateUnavailable"), pa("DataUnavailable", body % "AutoscalerLimitReached"),
           pa("ConfigurationNotValidated"), pa("SettingsInvalid"), pa("LeadInsufficient"), pa("NotEligible"),
           pa("DataUnavailable", "ML API returned status 503: not json"),
           pa("DataUnavailable", body.replace('"x"', '"history unavailable: connection refused"') % "DataUnavailable"),
           pa("HistoryInsufficient"), pa("CalibrationInsufficient"),             # a trust reason, nothing issued
           pa("DataUnavailable", "no forecast")]
    for p in ok:
        code, v = validated(p, Contracts)
        assert code == 0, (p, v)
    for p in bad:
        code, v = validated(p, Contracts)
        want = 2 if p["status"]["conditions"][0]["reason"] in PERMANENT else 3
        assert code == want, (p, v, want)
    assert validated({"status": {}}, Contracts)[0] == 1
    Contracts.lead = 900
    assert validated(ok[0], Contracts)[0] == 2
    good_pod = {"metadata": {"name": "op-1"}, "status": {"phase": "Running",
                "containerStatuses": [{"name": "operator", "ready": True, "restartCount": 0}]}}
    assert pods_problems({"items": [good_pod]}, 1, "operator") == []
    restarted = json.loads(json.dumps(good_pod))
    restarted["metadata"]["name"] = "op-2"
    restarted["status"]["containerStatuses"][0]["restartCount"] = 1
    assert pods_problems({"items": [good_pod, restarted]}, 2, "operator")      # a second replica restarted
    assert pods_problems({"items": [good_pod, good_pod]}, 1, "operator")       # one pod too many
    assert pods_problems({"items": []}, 1, "operator")                         # missing
    sidecar = json.loads(json.dumps(good_pod))
    sidecar["status"]["containerStatuses"].append({"name": "x", "ready": False, "restartCount": 0})
    assert pods_problems({"items": [sidecar]}, 1, "operator")                  # every container counts
    print("shadow_checks selftest: S6 accepts only the service's own answer with a missing-history or data reason "
          "(TLS, refused, limit, backend and configuration refusals fail); S7 checks every pod and container: OK")
    return 0


def main(argv) -> int:
    if argv[1] == "selftest":
        return selftest()
    if argv[1] == "validated":
        sys.path.insert(0, argv[3])
        from forecasting import contracts
        code, verdict = validated(json.load(open(argv[2])), contracts)
        with open(argv[4], "w") as fh:
            json.dump(verdict, fh, indent=1)
        return code
    if argv[1] == "pods":
        found = pods_problems(json.load(open(argv[2])), int(argv[3]), argv[4])
        print(json.dumps(found))
        return 2 if found else 0
    raise SystemExit(f"unknown command {argv[1]}")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
