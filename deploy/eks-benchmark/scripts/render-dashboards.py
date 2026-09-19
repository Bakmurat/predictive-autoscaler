#!/usr/bin/env python3
"""Turn ../../grafana/dashboards/*.json into Grafana-sidecar ConfigMaps (datasource uid 'prometheus')."""
import glob, json, os, sys
here = os.path.dirname(os.path.abspath(__file__))
src = os.path.join(here, "..", "..", "..", "grafana", "dashboards")
items = []
for f in sorted(glob.glob(os.path.join(src, "*.json"))):
    base = os.path.basename(f)[:-5]
    d = json.loads(open(f, encoding="utf-8").read().replace("${DS_PROMETHEUS}", "prometheus"))
    d["uid"] = "pa-" + base[:30]
    d["id"] = None
    if not d.get("title") or d["title"] == "Predictive Autoscaler":
        d["title"] = "Predictive Autoscaler - " + base
    items.append({"apiVersion": "v1", "kind": "ConfigMap",
                  "metadata": {"name": "dash-" + base.lower(), "namespace": "monitoring",
                               "labels": {"grafana_dashboard": "1"}},
                  "data": {base + ".json": json.dumps(d)}})
json.dump({"apiVersion": "v1", "kind": "List", "items": items}, sys.stdout)
