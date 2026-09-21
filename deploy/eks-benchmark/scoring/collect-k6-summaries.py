#!/usr/bin/env python3
"""Collect the one-line JSON summaries printed by the hourly k6 Jobs into k6-summaries/<target>.jsonl
(idempotent: keyed by job name). Run daily while the benchmark is up.
Usage: python3 collect-k6-summaries.py [--namespace demo] [--out k6-summaries]"""
import argparse, json, os, subprocess
ap = argparse.ArgumentParser(); ap.add_argument("--namespace", default="demo"); ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "k6-summaries"))
a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
jobs = json.loads(subprocess.check_output(["kubectl", "-n", a.namespace, "get", "jobs", "-l", "app=k6", "-o", "json"]))["items"]
seen = {}
for f in os.listdir(a.out):
    for line in open(os.path.join(a.out, f)):
        try: seen[json.loads(line)["job"]] = 1
        except Exception: pass
added = 0
for j in jobs:
    name = j["metadata"]["name"]; target = j["metadata"]["labels"].get("target", "unknown")
    if name in seen: continue
    st = j.get("status", {})
    if not (st.get("succeeded") or st.get("failed")): continue
    try:
        log = subprocess.check_output(["kubectl", "-n", a.namespace, "logs", f"job/{name}", "-c", "k6", "--tail=5"], stderr=subprocess.DEVNULL, text=True)
    except subprocess.CalledProcessError:
        log = ""
    summary = None
    for line in log.splitlines():
        if line.startswith("{") and '"k6-hour-summary"' in line:
            summary = json.loads(line)
    rec = {"job": name, "target": target, "succeeded": bool(st.get("succeeded")), "failed": st.get("failed", 0),
           "start": st.get("startTime"), "end": st.get("completionTime"), "summary": summary}
    with open(os.path.join(a.out, f"{target}.jsonl"), "a") as f: f.write(json.dumps(rec) + "\n")
    added += 1
print(f"added {added} summaries to {a.out}")
