#!/usr/bin/env python3
"""Soak recorder (soak/README.md "Recorded throughout"). Every --interval seconds it appends one JSON line per sample
to <out>/<UTC day>.jsonl:
  - every PredictiveAutoscaler in pa-soak: mode, current/calculated/applied/ready, lastScaleTime, metric-source hash,
    conditions (type, status, reason);
  - every Deployment in pa-soak: spec.replicas, generation, readyReplicas;
  - Events in pa-soak and predictive-autoscaler that are new since the previous sample;
  - restart counts and last termination reasons (OOMKilled, Error) of the operator and forecaster pods.
A sample that fails is written as {"ok": false, "error": …}: evidence gaps are recorded, never filled.
Read-only: only kubectl get. Run it next to the cluster (it uses the current kubeconfig context, which must be kind-*).
  record.py --out ~/soak/records [--interval 30]
"""
import argparse, json, os, subprocess, sys, time


def kget(*args):
    out = subprocess.run(["kubectl", "get", *args, "-o", "json"], capture_output=True, text=True, timeout=30)
    if out.returncode:
        raise RuntimeError(out.stderr.strip()[:300])
    return json.loads(out.stdout)["items"]


def sample(seen):
    pas = [{"name": p["metadata"]["name"], "mode": p["spec"].get("mode"), "gen": p["metadata"].get("generation"),
            **{k: p.get("status", {}).get(k) for k in ("currentReplicas", "calculatedReplicas", "forecastReplicas",
                                                         "stabilizedReplicas", "appliedReplicas", "readyReplicas",
                                                         "lastScaleTime", "lastPrediction", "observedGeneration")},
            "query_sha": (p.get("status", {}).get("metricSource") or {}).get("sha256"),
            "conditions": {c["type"]: [c["status"], c.get("reason")] for c in p.get("status", {}).get("conditions", [])}}
           for p in kget("predictiveautoscalers.autoscaling.devkuban.com", "-n", "pa-soak")]
    deps = [{"name": d["metadata"]["name"], "replicas": d["spec"].get("replicas"), "gen": d["metadata"].get("generation"),
             "ready": d.get("status", {}).get("readyReplicas", 0)} for d in kget("deployments", "-n", "pa-soak")]
    events = []
    for ns in ("pa-soak", "predictive-autoscaler"):
        for e in kget("events", "-n", ns):
            uid = e["metadata"]["uid"] + ":" + str(e.get("count") or e.get("series", {}).get("count") or 1)
            if uid in seen:
                continue
            seen.add(uid)
            events.append({"ns": ns, "at": e.get("lastTimestamp") or e.get("eventTime"), "type": e.get("type"),
                           "reason": e.get("reason"), "object": e["involvedObject"].get("name"),
                           "message": (e.get("message") or "")[:200]})
    pods = []
    for p in kget("pods", "-n", "predictive-autoscaler"):
        for c in p.get("status", {}).get("containerStatuses", []):
            last = (c.get("lastState") or {}).get("terminated") or {}
            pods.append({"pod": p["metadata"]["name"], "container": c["name"], "restarts": c.get("restartCount"),
                         "last_reason": last.get("reason"), "ready": c.get("ready")})
    return {"autoscalers": pas, "deployments": deps, "events": events, "pods": pods}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=int, default=30)
    a = ap.parse_args()
    ctx = subprocess.run(["kubectl", "config", "current-context"], capture_output=True, text=True).stdout.strip()
    if not ctx.startswith("kind-"):
        sys.exit(f"refusing: current context {ctx!r} is not a kind cluster")
    os.makedirs(a.out, exist_ok=True)
    seen = set()
    while True:
        t0 = time.time()
        rec = {"at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t0)), "context": ctx}
        try:
            rec.update(ok=True, **sample(seen))
        except Exception as e:
            rec.update(ok=False, error=str(e)[:300])
        with open(os.path.join(a.out, rec["at"][:10] + ".jsonl"), "a") as fh:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        if len(seen) > 200000:
            seen.clear()
        time.sleep(max(1, a.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()
