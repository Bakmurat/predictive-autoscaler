"""Validate current-generation quickstart outcomes, rather than pod phase alone."""
import datetime
import json
import sys


def check(data, phase, since):
    pa = next(x for x in data["items"] if x["kind"] == "PredictiveAutoscaler")
    deploy = next(x for x in data["items"] if x["kind"] == "Deployment")
    gen = pa["metadata"]["generation"]
    status = pa.get("status", {})
    conditions = {c["type"]: c for c in status.get("conditions", [])}

    def condition(name, value):
        c = conditions.get(name, {})
        return c.get("status") == value and c.get("observedGeneration") == gen

    assert status.get("observedGeneration") == gen
    assert status.get("metricSource", {}).get("observedGeneration") == gen
    assert condition("Ready", "True") and condition("TelemetryAvailable", "True")
    assert condition("ConflictDetected", "False")
    want = status.get("calculatedReplicas")
    assert isinstance(want, int) and want >= 2
    actual = deploy["spec"]["replicas"]
    if phase in ("recommend", "forecast", "stopped"):
        assert status.get("mode") == "Recommend" and condition("ScalingActive", "False")
        if phase != "stopped":
            assert actual == 1, "Recommend changed replicas"
    if phase == "recommend":
        assert condition("ForecastAvailable", "False"), "fresh install unexpectedly has a model"
    if phase in ("forecast", "active"):
        assert condition("ForecastAvailable", "True")
        issued = datetime.datetime.fromisoformat(status["lastPrediction"].replace("Z", "+00:00"))
        assert issued > datetime.datetime.fromisoformat(since.replace("Z", "+00:00"))
        assert (datetime.datetime.now(datetime.timezone.utc) - issued).total_seconds() < 600
    if phase in ("active", "active-reactive"):
        assert status.get("mode") == "Active" and condition("ScalingActive", "True")
        assert actual == want and status.get("appliedReplicas") == actual
        assert deploy.get("status", {}).get("readyReplicas") == actual


if __name__ == "__main__":
    try:
        check(json.load(sys.stdin), sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "")
    except (AssertionError, KeyError, StopIteration, ValueError) as exc:
        print(f"waiting for {sys.argv[1]}: {exc}", file=sys.stderr)
        sys.exit(1)
