"""deploy/prodcluster/freeze_capture.py v3: freeze gates, redaction and audit before writing, telemetry completeness
(protocol P6 step 5; Codex r53 BLOCKERs 1-4, r54-r56 BLOCKERs 1-2 and their SHOULD-FIX items)."""
import calendar
import copy
import importlib.util
import json
import os
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "..", "..", "deploy", "prodcluster", "freeze_capture.py")
SPEC = importlib.util.spec_from_file_location("freeze_capture", PATH)
fz = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fz)

API, OP, COMMIT = "sha256:" + "a" * 64, "sha256:" + "b" * 64, "c" * 40
ENV = {"ML_API_DIGEST": API, "OPERATOR_DIGEST": OP, "GIT_COMMIT_VALUE": COMMIT}
CFG = {"ENSEMBLE_HISTORY_HOURS": 360}
WORKERS = ["w1", "w2"]


def container(name, image, env=None, requests=None, **extra):
    return dict({"name": name, "image": image, "env": [{"name": k, "value": v} for k, v in (env or {}).items()],
                 "resources": {"requests": requests or {}}}, **extra)


# ------------------------------------------------------------------ effective environment (r53 B2, r54 B1)
def ml_engine(api=None, trainer=None):
    a = container("api", "h/ml-api@" + API, {"GIT_COMMIT": COMMIT, "ENSEMBLE_HISTORY_HOURS": "360"})
    t = container("trainer", "h/ml-api@" + API, {"GIT_COMMIT": COMMIT})
    a.update(api or {})
    t.update(trainer or {})
    deploys = [{"metadata": {"name": "ml-api"}, "spec": {"template": {"spec": {"containers": [a]}}}},
               {"metadata": {"name": "predictive-operator"},
                "spec": {"template": {"spec": {"containers": [container("operator", "h/op@" + OP)]}}}}]
    cron = [{"metadata": {"name": "ml-training"},
             "spec": {"schedule": "0 */6 * * *", "jobTemplate": {"spec": {"template": {"spec": {"containers": [t]}}}}}}]
    return deploys, cron


def test_effective_env_follows_kubernetes_precedence_and_marks_opaque_values():
    c = {"envFrom": [{"configMapRef": {"name": "one"}}, {"configMapRef": {"name": "two"}, "prefix": "P_"},
                     {"configMapRef": {"name": "absent", "optional": True}}],
         "env": [{"name": "A", "value": "env-wins"},
                 {"name": "B", "valueFrom": {"configMapKeyRef": {"name": "one", "key": "B"}}},
                 {"name": "S", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}},
                 {"name": "F", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}]}
    env, unresolved, sources = fz.effective_env(c, {"one": {"A": "cm", "B": "b", "X": "1"}, "two": {"X": "2"}})
    assert env["A"] == "env-wins" and env["B"] == "b" and env["X"] == "1" and env["P_X"] == "2"
    assert env["S"] == "<secret s/k>" and env["F"] == "<field metadata.name>" and len(unresolved) == 2
    assert sources == {"B": "one", "X": "one", "P_X": "two"}              # A came from env, not the ConfigMap
    _, unresolved, _ = fz.effective_env({"envFrom": [{"secretRef": {"name": "s"}}, {"configMapRef": {"name": "gone"}}],
                                         "env": [{"name": "R", "value": "$(OTHER)"}]}, {})
    assert len(unresolved) == 3


def test_envelope_gate_sees_configmap_backed_settings_and_running_pods():
    assert fz.gate_ml_engine(*ml_engine(), {}, ENV, CFG)[:2] == ([], [])
    bad = ml_engine(api={"env": [{"name": "GIT_COMMIT", "value": COMMIT},
                                 {"name": "ENSEMBLE_HISTORY_HOURS",
                                  "valueFrom": {"configMapKeyRef": {"name": "knobs", "key": "hours"}}}]},
                    trainer={"envFrom": [{"configMapRef": {"name": "knobs"}}]})
    _, envelope, _ = fz.gate_ml_engine(*bad, {"knobs": {"hours": "240", "TRAINING_HOURS": "48"}}, ENV, CFG)
    assert any("ENSEMBLE_HISTORY_HOURS '240'" in e for e in envelope) and any("TRAINING_HOURS" in e for e in envelope)
    assert any("Secret" in e for e in fz.gate_ml_engine(*ml_engine(trainer={"envFrom": [{"secretRef": {"name": "s"}}]}),
                                                         {}, ENV, CFG)[1])
    # r54 probe: the running pod has 240 while the template declares 360
    pod_c = container("api", "h/ml-api@" + API, {"GIT_COMMIT": COMMIT, "ENSEMBLE_HISTORY_HOURS": "240"})
    assert any("pod api-1" in e for e in fz.gate_ml_engine(*ml_engine(), {}, ENV, CFG, [("api-1", pod_c)])[1])
    images, _, _ = fz.gate_ml_engine(*ml_engine(api={"image": "h/ml-api@sha256:" + "e" * 64}), {}, ENV, CFG)
    assert len(images) == 1


# ------------------------------------------------------------------ runtime binding (r53 B1, r54 B1)
def deployment(name, image, replicas=1, revision="3", env=None, **status):
    st = dict({"observedGeneration": 5, "replicas": replicas, "updatedReplicas": replicas, "readyReplicas": replicas,
               "availableReplicas": replicas}, **status)
    return {"metadata": {"name": name, "namespace": "ns", "uid": f"d-{name}", "generation": 5,
                         "annotations": {"deployment.kubernetes.io/revision": revision}},
            "spec": {"replicas": replicas, "template": {"spec": {
                "containers": [dict({"name": "c", "image": image}, **({"env": env} if env else {}))],
                "volumes": [{"name": "data", "configMap": {"name": "x", "defaultMode": 420}}]}}},
            "status": st}


def replicaset(name, deploy, revision):
    return {"metadata": {"name": name, "uid": f"rs-{name}", "ownerReferences": [{"uid": f"d-{deploy}"}],
                         "annotations": {"deployment.kubernetes.io/revision": revision}}}


def pod(name, rs, image, image_id, phase="Running", ready=True, statuses=True, sidecar=False, env=None,
        started="2026-10-08T00:00:00Z"):
    c = dict({"name": "c", "image": image,
              "volumeMounts": [{"name": "kube-api-access-ab12c", "mountPath": "/var/run/secrets"}]},
             **({"env": env} if env else {}))
    containers = [c] + ([{"name": "istio-proxy", "image": "proxyv2:1.26.0"}] if sidecar else [])
    st = [{"name": "c", "imageID": image_id, "ready": ready, "restartCount": 0,
           "state": {"running": {"startedAt": started}}}]
    st += [{"name": "istio-proxy", "imageID": "docker.io/istio/proxyv2@sha256:" + "1" * 64, "ready": ready}] if sidecar else []
    vols = [{"name": "data", "configMap": {"name": "x", "defaultMode": 420}}, {"name": "kube-api-access-ab12c"}]
    return {"metadata": {"name": name, "uid": f"p-{name}", "ownerReferences": [{"uid": f"rs-{rs}"}]},
            "spec": {"nodeName": "w1", "containers": containers, "volumes": vols},
            "status": {"phase": phase, "containerStatuses": st if statuses else []}}


def runtime_world():
    img = "h/ml-api@" + API
    deploys = [deployment("ml-api", img), deployment("nginx-test", "nginx@sha256:" + "2" * 64, replicas=2)]
    rss = [replicaset("api-new", "ml-api", "3"), replicaset("api-old", "ml-api", "2"),
           replicaset("ngx", "nginx-test", "3")]
    pods = [pod("api-1", "api-new", img, "h/ml-api@" + API),
            pod("ngx-1", "ngx", "nginx@sha256:" + "2" * 64, "nginx@sha256:" + "2" * 64, sidecar=True),
            pod("ngx-2", "ngx", "nginx@sha256:" + "2" * 64, None, phase="Pending", statuses=False)]
    return deploys, rss, pods


def test_runtime_binds_pods_to_the_current_replicaset():
    deploys, rss, pods = runtime_world()
    problems, rows = fz.gate_runtime(deploys, rss, pods)
    assert problems == [] and {r["pod"] for r in rows} == {"api-1", "ngx-1", "ngx-2"}   # autoscaling Pending allowed
    assert any(c["injected"] for r in rows for c in r["containers"])
    old = pods + [pod("api-0", "api-old", "h/ml-api@sha256:" + "9" * 64, "h/ml-api@sha256:" + "9" * 64)]
    assert any("old ReplicaSet" in p for p in fz.gate_runtime(deploys, rss, old)[0])
    no_status = copy.deepcopy(pods)
    no_status[0]["status"]["containerStatuses"] = []
    assert any("reports status" in p for p in fz.gate_runtime(deploys, rss, no_status)[0])
    unready = copy.deepcopy(pods)
    unready[0]["status"]["containerStatuses"][0]["ready"] = False
    assert any("not Ready" in p for p in fz.gate_runtime(deploys, rss, unready)[0])
    rolling = copy.deepcopy(deploys)
    rolling[1]["status"]["updatedReplicas"] = 1
    rolling[0]["status"]["observedGeneration"] = 4
    assert len(fz.gate_runtime(rolling, rss, pods)[0]) == 2
    wrong = copy.deepcopy(pods)
    wrong[1]["status"]["containerStatuses"][0]["imageID"] = "nginx@sha256:" + "3" * 64
    assert any("runs nginx@sha256:333" in p for p in fz.gate_runtime(deploys, rss, wrong)[0])


def test_r54_runtime_compares_the_admitted_container_configuration():
    deploys, rss, pods = runtime_world()
    deploys[0]["spec"]["template"]["spec"]["containers"][0]["env"] = [{"name": "ENSEMBLE_HISTORY_HOURS", "value": "360"}]
    drifted = copy.deepcopy(pods)
    drifted[0]["spec"]["containers"][0]["env"] = [{"name": "ENSEMBLE_HISTORY_HOURS", "value": "240"}]
    assert any("c.env differs" in p for p in fz.gate_runtime(deploys, rss, drifted)[0])
    renamed = copy.deepcopy(pods)                     # r54 probe: the template container replaced by another name
    renamed[0]["spec"]["containers"][0]["name"] = "impostor"
    renamed[0]["status"]["containerStatuses"][0]["name"] = "impostor"
    problems = fz.gate_runtime(deploys, rss, renamed)[0]
    assert any("template container c absent" in p for p in problems)
    assert any("container impostor not in the template" in p for p in problems)
    mounted = copy.deepcopy(pods)
    mounted[1]["spec"]["containers"][0]["volumeMounts"].append({"name": "extra", "mountPath": "/x"})
    mounted[1]["spec"]["volumes"][0]["configMap"]["name"] = "other"
    problems = fz.gate_runtime(deploys, rss, mounted)[0]
    assert any("volumeMounts differ" in p for p in problems) and any("volume data differs" in p for p in problems)
    for f, v in (("args", ["--x"]), ("command", ["sh"]), ("envFrom", [{"configMapRef": {"name": "k"}}])):
        changed = copy.deepcopy(pods)
        changed[0]["spec"]["containers"][0][f] = v
        assert any(f"c.{f} differs" in p for p in fz.gate_runtime(deploys, rss, changed)[0])


def test_r55_configmap_write_provenance_is_never_inferred_from_creation():
    md = {"creationTimestamp": "2026-10-01T00:00:00Z"}
    # r56 probe: an immutable ConfigMap whose data/immutability was written after the container started
    late = {"metadata": dict(md, managedFields=[{"time": "2026-10-08T02:00:00Z"}]), "immutable": True}
    assert fz.cm_provenance(late)["updated"] == "2026-10-08T02:00:00Z"
    assert fz.cm_provenance({"metadata": dict(md), "immutable": True})["updated"] is None    # immutable, no history
    hist = {"metadata": dict(md, managedFields=[{"time": "2026-10-05T05:20:30Z"}, {"time": "2026-10-03T00:00:00Z"}])}
    assert fz.cm_provenance(hist) == {"updated": "2026-10-05T05:20:30Z", "provenance": "latest managedFields write"}
    assert fz.cm_provenance({"metadata": dict(md)})["updated"] is None      # mutable, no history: unknown
    deploys, rss, pods = runtime_world()
    ref = [{"name": "URL", "valueFrom": {"configMapKeyRef": {"name": "cfg", "key": "url"}}}]
    deploys[0]["spec"]["template"]["spec"]["containers"][0]["env"] = ref
    pods[0]["spec"]["containers"][0]["env"] = ref
    unknown = {("ns", "cfg"): dict(fz.cm_provenance({"metadata": dict(md)}), data={"url": "u"})}
    assert any("from ConfigMap cfg written None" in p for p in fz.gate_runtime(deploys, rss, pods, unknown)[0])


def test_r54_configmap_env_written_after_the_container_started_is_not_trusted():
    deploys, rss, pods = runtime_world()
    ref = [{"name": "URL", "valueFrom": {"configMapKeyRef": {"name": "cfg", "key": "url"}}}]
    deploys[0]["spec"]["template"]["spec"]["containers"][0]["env"] = ref
    pods[0]["spec"]["containers"][0]["env"] = ref
    before = {("ns", "cfg"): {"data": {"url": "u"}, "updated": "2026-10-07T23:00:00Z"}}
    after = {("ns", "cfg"): {"data": {"url": "u"}, "updated": "2026-10-08T01:00:00Z"}}
    assert fz.gate_runtime(deploys, rss, pods, before)[0] == []
    assert any("written 2026-10-08T01:00:00Z after" in p for p in fz.gate_runtime(deploys, rss, pods, after)[0])


# ------------------------------------------------------------------ declared processes (r54 SHOULD-FIX 4)
SLOT0 = calendar.timegm((2026, 10, 8, 18, 0, 0))


def job(cj, slot, ok=True, image="h/ml-api@" + API, finished=True):
    j = {"metadata": {"name": f"{cj}-{slot // 60}", "uid": f"j-{cj}-{slot}",
                      "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(slot)),
                      "ownerReferences": [{"kind": "CronJob", "name": cj}]},
         "spec": {"template": {"spec": {"containers": [container("x", image, {"GIT_COMMIT": COMMIT})]}}},
         "status": {"conditions": [{"type": "Complete" if ok else "Failed", "status": "True"}] if finished else []}}
    return j


def tpod(j):
    return {"metadata": {"name": "t" + j["metadata"]["uid"], "uid": "p" + j["metadata"]["uid"],
                         "ownerReferences": [{"uid": j["metadata"]["uid"]}]}, "spec": {"containers": []},
            "status": {"phase": "Succeeded", "containerStatuses": [{"name": "x", "imageID": "h/ml-api@" + API}]}}


ART = "6eb29669e30e" + "3" * 52


def tcheck(slot, job_name, art=ART, **over):
    d = {"checker": "training-check.sh v3.5.3", "slot": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(slot)),
         "job": job_name, "ok": True, "status": "verified", "artifact_sha256": art,
         "legs": {l: {"status": "ok"} for l in fz.LEGS}, "first_issuance": {"artifact_sha256": art}}
    d.update(over)
    return d


def test_declared_processes_bind_slot_archive_pair_and_reload():
    tr = job("ml-training", SLOT0)
    jobs = [tr, job("evidence-archive", SLOT0 + 1200), job("load-evidence", SLOT0 + 840)]
    tmpl = tr["spec"]["template"]["spec"]["containers"][0]
    log = 'x\n{"errors": [], "pair": {"key": "20261008T180000Z-6eb29669e30e", "result": "archived"}}\n'
    api = "INFO:api.main:Reloaded model nginx-test_requests from disk (file updated); artifact sha256=6eb29669e30e\n"
    now = SLOT0 + 4 * 3600
    prev = tcheck(SLOT0 - 6 * 3600, f"ml-training-{(SLOT0 - 6 * 3600) // 60}", art="1" * 64,
                  first_issuance={"artifact_sha256": "1" * 64})
    tc = [prev, tcheck(SLOT0, tr["metadata"]["name"])]
    gp = lambda jobs=jobs, pods=None, log=log, api=api, tmpl=tmpl, now=now, tc=tc: fz.gate_processes(
        jobs, [tpod(tr)] if pods is None else pods, ENV, log, api, tmpl, now, tc)
    problems, summary = gp()
    assert problems == [] and summary["ml-training"]["expected_slot"] == SLOT0
    assert summary["artifact"]["training_check_sha256"] == ART
    assert any("slot" in p for p in gp(now=now + 3 * 3600)[0])                         # the 00Z training missing
    # r66: a newer training still running is pending and fails — an older slot never substitutes
    running = job("ml-training", SLOT0 + 6 * 3600, finished=False)
    p2, s2 = gp(jobs=jobs + [running], now=SLOT0 + 6 * 3600 + 120)
    assert any("still pending" in p for p in p2) and s2["ml-training"]["pending"] == [running["metadata"]["name"]]
    assert any("previous slot" in p for p in gp(tc=[tc[1]])[0])                 # D-1097: two preceding slots
    assert any("previous slot" in p for p in gp(tc=[dict(prev, status="incomplete_historical", ok=False), tc[1]])[0])
    assert any("exactly two" in p for p in gp(tc=[prev, tc[1], tc[1]])[0])
    other = api.replace("6eb29669e30e", "0123456789ab")
    assert any("last reload" in p for p in gp(api=other)[0])
    # r55 probe: another application's reload line with the matching prefix does not count
    unrelated = other + "INFO:api.main:Reloaded model myapptwo_requests from disk; artifact sha256=6eb29669e30e\n"
    assert any("last reload" in p for p in gp(api=unrelated)[0])
    restarted = "INFO:api.main:Loaded model for nginx-test_requests (age: 1.8h, sha256=6eb29669e30e, training_cutoff=x)\n"
    assert gp(api=other + restarted)[0] == []                    # a pod restart loads the artifact at start-up
    assert any("last reload" in p for p in gp(api=restarted + other)[0])         # the later line wins
    stale = log.replace("20261008T180000Z", "20261008T120000Z")
    assert any("expected slot" in p for p in gp(log=stale)[0])
    assert any("evidence-archive" in p for p in gp(log='{"errors": ["boom"], "pair": {}}')[0])
    assert any("differs from the CronJob template" in p for p in gp(tmpl=dict(tmpl, args=["--other"]))[0])
    assert any("imageIDs" in p for p in gp(pods=[])[0])


def test_r55_processes_need_the_four_leg_training_check_for_the_slot():
    tr = job("ml-training", SLOT0)
    jobs = [tr, job("evidence-archive", SLOT0 + 1200), job("load-evidence", SLOT0 + 840)]
    tmpl = tr["spec"]["template"]["spec"]["containers"][0]
    log = '{"errors": [], "pair": {"key": "20261008T180000Z-6eb29669e30e", "result": "archived"}}\n'
    api = "Reloaded model nginx-test_requests from disk (file updated); artifact sha256=6eb29669e30e\n"
    prev = tcheck(SLOT0 - 6 * 3600, f"ml-training-{(SLOT0 - 6 * 3600) // 60}")
    run = lambda tc: [p for p in fz.gate_processes(jobs, [tpod(tr)], ENV, log, api, tmpl, SLOT0 + 4 * 3600,
                                                   [prev] + ([tc] if tc is not None else []))[0]
                      if not p.startswith("expected exactly two")]
    name = tr["metadata"]["name"]
    assert run(tcheck(SLOT0, name)) == []
    assert run(None) == ["no training-check result for the expected slot"]
    bad = [tcheck(SLOT0 - 6 * 3600, name), tcheck(SLOT0, "ml-training-1"), tcheck(SLOT0, name, checker="other"),
           tcheck(SLOT0, name, status="incomplete_historical", ok=False),
           tcheck(SLOT0, name, legs={l: {"status": "ok"} for l in fz.LEGS[:3]}),
           tcheck(SLOT0, name, art="6eb29669e30e"), tcheck(SLOT0, name, first_issuance={"artifact_sha256": "x"}),
           tcheck(SLOT0, name, art="0123456789ab" + "0" * 52, first_issuance={"artifact_sha256": "0123456789ab" + "0" * 52})]
    for tc in bad:
        assert run(tc), tc


EXPECT = {"context": "prodcluster", "fetcher_sha256": "f" * 64, "collector_sha256": "c" * 64,
          "inputs_sha256": {"terminations": "t" * 64, "approvals": "a" * 64}}
SOURCE = {"context": "prodcluster", "pvc": "load-evidence-pvc", "pod": "load-attempts-fetch-052504-ab", "node": "w1"}


def hour_of(path):
    m = fz.FILE_NAME.match(path)
    t = m.group(2)
    return f"{t[:4]}-{t[4:6]}-{t[6:8]}T{t[9:11]}:{t[11:13]}:00Z"


def fetch_output(files, hours, extractor="fetch_load_attempts.sh v1", tamper=False, rec_over=None, entries=None):
    """A fetch_load_attempts.sh output: receipt, one line per file text, trailer."""
    rec = {"receipt": dict({"extractor": extractor, "extractor_sha256": "f" * 64, "hours": hours, "source": SOURCE,
                            "files": entries if entries is not None else [
                                {"hour": hour_of(p), "path": p, "bytes": len(t.encode()),
                                 "sha256": fz.sha_bytes(t.encode())} for p, t in files]}, **(rec_over or {}))}
    body = "\n".join([json.dumps(rec)] + [json.dumps({"file": p, "text": t}) for p, t in files]) + "\n"
    trailer = {"trailer": {"bytes": len(body.encode()), "sha256": fz.sha_bytes(body.encode())}}
    if tamper:
        body = body.replace("PASS", "PASs", 1)
    return (body + json.dumps(trailer) + "\n").encode()


def attempt(h, final=True, status="PASS", q=None, arms=fz.ARMS, collector_sha="c" * 64, gen=None):
    q = (status == "PASS" and final) if q is None else q
    return json.dumps([{"app": a, "hour_start": h, "status": status, "maturity": {"finalized": final},
                        "qualification": {"qualifies": q}, "collector": "collect_load_evidence.py v8",
                        "collector_sha256": collector_sha, "inputs_sha256": EXPECT["inputs_sha256"],
                        "generator_pod": (gen or {}).get(a, f"k6-{a}-abcdef1234-xyz12")} for a in arms])


H1, H2 = "2026-10-07T21:00:00Z", "2026-10-07T22:00:00Z"


def test_r59_qualification_reads_final_attempts_and_fails_closed_on_conflicts():
    t1, t2 = fz.hour_tag(H1), fz.hour_tag(H2)
    a1, b1, a2, b2 = (f"attempts/{t}-20261008T0{i}1500Z-1.json" for t, i in ((t1, 0), (t1, 1), (t2, 0), (t2, 1)))
    good = [(a1, attempt(H1, final=False, q=False)),                           # provisional False, then final True
            (b1, attempt(H1)), (a2, attempt(H2)), (f"{t2}.json", attempt(H2))]
    gq = lambda files, hours=(H1, H2), **kw: fz.gate_qualification(fetch_output(files, list(hours), **kw), list(hours),
                                                                    EXPECT)
    problems, summary = gq(good)
    assert problems == [] and len(summary[H1]["attempts"]) == 2
    for bad_final in (attempt(H2, status="FAIL"), attempt(H2, status="INCOMPLETE"), attempt(H2, q=False),
                      attempt(H2, collector_sha="0" * 64)):                    # r60: rows from another collector
        for order in ((a2, b2), (b2, a2)):
            files = [good[1], (order[0], attempt(H2)), (order[1], bad_final)]
            assert any(f"{H2} " in p for p in gq(files)[0])
    assert any("no final evaluation" in p for p in gq([good[1], (a2, attempt(H2, final=False, q=False))])[0])
    assert any("arms" in p for p in gq([good[1], (a2, attempt(H2, arms=fz.ARMS[:5]))])[0])
    assert any("row for" in p for p in gq([good[1], (a2, attempt(H1))])[0])
    for broken in (lambda r: r.pop("maturity"), lambda r: r.update(maturity="final"),        # r61: malformed maturity
                   lambda r: r.update(maturity={"finalized": "yes"}), lambda r: r["maturity"].pop("finalized")):
        rows = json.loads(attempt(H2))
        broken(rows[0])
        assert any("no boolean maturity.finalized" in p for p in gq([good[1], (a2, json.dumps(rows))])[0])
    assert gq(good, tamper=True)[0] == ["the qualification fetch does not match its trailer"]
    assert any("not two or more consecutive" in p for p in gq(good, hours=(H1,))[0])
    assert fz.gate_qualification(b"", [], EXPECT)[0] == ["no qualification attempts or hours given"]


def test_r60_the_fetch_receipt_must_name_the_frozen_fetcher_and_source():
    t1, t2 = fz.hour_tag(H1), fz.hour_tag(H2)
    good = [(f"attempts/{t1}-20261008T001500Z-1.json", attempt(H1)), (f"attempts/{t2}-20261008T011500Z-1.json", attempt(H2))]
    gq = lambda **kw: fz.gate_qualification(fetch_output(good, [H1, H2], **kw), [H1, H2], EXPECT)[0]
    assert gq() == []
    for over in ({"extractor": "fetch_load_attempts.sh v1OTHER"}, {"extractor_sha256": "0" * 64},   # r60 probe
                 {"source": dict(SOURCE, context="other")}, {"source": dict(SOURCE, pvc="other-pvc")},
                 {"source": dict(SOURCE, pod="")}, {"source": dict(SOURCE, node="")}, {"source": "x"}):
        assert gq(rec_over=over), over
    entry = lambda p, t, **o: dict({"hour": hour_of(p), "path": p, "bytes": len(t.encode()),
                                    "sha256": fz.sha_bytes(t.encode())}, **o)
    dup = [entry(*good[0]), entry(*good[0]), entry(*good[1])]
    assert any("twice" in p for p in gq(entries=dup))
    assert any("is for hour" in p for p in gq(entries=[entry(*good[0], hour=H2), entry(*good[1])]))
    assert any("malformed receipt entry" in p for p in gq(entries=[entry(*good[0], path="../x.json"), entry(*good[1])]))
    assert any("malformed receipt entry" in p for p in gq(entries=[entry(*good[0], sha256="short"), entry(*good[1])]))


# ------------------------------------------------------------------ redaction and audit (r53 B3, r54 B2)
def test_credentials_in_args_urls_and_env_are_redacted_and_references_kept():
    obj = {"spec": {"automountServiceAccountToken": False, "imagePullSecrets": [{"name": "harbor-pull"}],
                    "volumes": [{"secret": {"secretName": "tls"}}],
                    "containers": [{"args": ["-httpAuth.password=hunter2", "-retentionPeriod=30d",
                                             "--tokenFile=/etc/token"],
                                    "env": [{"name": "DB_PASSWORD", "value": "hunter2"},
                                            {"name": "VM_URL", "value": "http://u:pw@vm:8481/x"},
                                            {"name": "API_TOKEN", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}},
                                            {"name": "LOG_LEVEL", "value": "INFO"}]}]},
           "data": {"accessKey": "AKIA-not-real"}, "note": "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----"}
    found = fz.redact(obj, [])
    text = json.dumps(obj)
    assert "hunter2" not in text and "pw@" not in text and "AKIA" not in text and "abc" not in text
    assert len(found) == 5 and fz.audit_text(text) == [] and fz.audit_json(obj) == []
    assert "-retentionPeriod=30d" in text and "--tokenFile=/etc/token" in text and '"INFO"' in text
    assert obj["spec"]["imagePullSecrets"] == [{"name": "harbor-pull"}] and "tls" in text


def test_r54_path_looking_and_quoted_credentials_are_redacted():
    obj = {"env": [{"name": "DB_PASSWORD", "value": "/not/a/path/really"}],
           "args": ['--password="quoted secret"', "-x.token='single'", "--secret=$(FROM_ENV)"]}
    fz.redact(obj, [])
    text = json.dumps(obj)
    assert "/not/a/path" not in text and "quoted secret" not in text and "single" not in text and "FROM_ENV" not in text
    assert fz.audit_text(text) == [] and fz.audit_json(obj) == []
    diff = '+  password: "FAKE_DIFF_MARKER"\n+  token: \'FAKE2\'\n'
    out = fz.redact_text(diff, [], "d")
    assert "FAKE_DIFF_MARKER" not in out and "FAKE2" not in out and fz.audit_text(out) == []


def test_r54_audit_is_independent_and_reads_the_serialized_bytes():
    # literals the audit must catch on its own, including inside JSON-escaped strings
    for leak in ('{"args": ["--password=\\"x\\""]}', '{"note": "password: hunter2"}', '{"u": "http://a:b@h/"}',
                 '{"k": "-----BEGIN EC PRIVATE KEY-----"}', '{"env": [{"name": "MY_TOKEN", "value": "/x"}]}'):
        assert fz.audit_text(leak) or fz.audit_json(json.loads(leak)), leak
    for fine in ('{"args": ["--password=\\"<redacted>\\""]}', '{"secretName": "tls"}', '{"tokenFile": "/etc/t"}',
                 '{"automountServiceAccountToken": false}', '{"name": "istio-token"}'):
        assert fz.audit_text(fine) == [] and fz.audit_json(json.loads(fine)) == [], fine


def test_diff_text_is_redacted_including_yaml_env_pairs():
    diff = ("-        - name: DB_PASSWORD\n-          value: hunter2\n+        - name: DB_PASSWORD\n"
            "+          value: s3cr3t\n+        - name: LOG_LEVEL\n+          value: INFO\n"
            "+        args: [\"-httpAuth.password=x1\"]\n+  url: https://a:b@host/\n")
    found = []
    out = fz.redact_text(diff, found, "d")
    assert "hunter2" not in out and "s3cr3t" not in out and "x1" not in out and "a:b@" not in out
    assert "value: INFO" in out and len(found) == 4 and fz.audit_text(out) == []


def test_staging_reserves_audits_and_publishes_without_replacing(tmp_path):
    out = str(tmp_path / "freeze")
    st = fz.Staging(out)
    assert os.path.isdir(out) and os.listdir(out) == []                 # the reservation
    with pytest.raises(FileExistsError):
        fz.Staging(out)                                                 # a second capture cannot take it
    st.json("objects/x.json", {"args": ['--password="abc"'], "env": [{"name": "TOKEN", "value": "/t"}]})
    data = open(os.path.join(st.dir, "objects/x.json")).read()
    assert "abc" not in data and '"/t"' not in data and len(st.found) == 2 and fz.audit_text(data) == []
    st.text("rendered/a.diff", "+  password: plain\n")
    assert "plain" not in open(os.path.join(st.dir, "rendered/a.diff")).read()
    st.publish({"verdict": "FAIL"})
    assert not os.path.exists(st.dir) and json.load(open(os.path.join(out, "COMPLETE.json")))["sha256sums_sha256"]
    assert "objects/x.json" in open(os.path.join(out, "SHA256SUMS")).read()
    st2 = fz.Staging(str(tmp_path / "other"))
    st2.abort()
    assert not os.path.exists(str(tmp_path / "other"))                  # an aborted run releases its reservation


# ------------------------------------------------------------------ telemetry (r53 B4, r54 SHOULD-FIX 3)
def node(name, ip, **cond):
    c = dict({"Ready": "True", "DiskPressure": "False", "MemoryPressure": "False", "PIDPressure": "False"}, **cond)
    return {"metadata": {"name": name, "labels": {"predictive-bench/ml-node": "true", "other": "x"}}, "spec": {},
            "status": {"conditions": [{"type": k, "status": v} for k, v in c.items()],
                       "addresses": [{"type": "InternalIP", "address": ip}]}}


def test_nodes_need_every_frozen_worker():
    problems, out = fz.gate_nodes([node("w1", "10.0.0.1"), node("w2", "10.0.0.2"), node("cp", "10.0.0.9", Ready="False")],
                                  WORKERS)
    assert problems == [] and out[0]["labels"] == {"predictive-bench/ml-node": "true"}
    assert fz.gate_nodes([], WORKERS)[0] == ["frozen worker w1 missing", "frozen worker w2 missing"]
    assert fz.gate_nodes([node("w1", "10.0.0.1", DiskPressure="True"), node("w2", "10.0.0.2")], WORKERS)[0] == \
        ["w1 DiskPressure=True"]


def disk_for(now, **over):
    base = {"10.0.0.1": {"size": 100.0, "avail": 25.0, "size_ts": now - 20, "avail_ts": now - 20},
            "10.0.0.2": {"size": 100.0, "avail": 50.0, "size_ts": now - 30, "avail_ts": now - 30}}
    base["10.0.0.1"].update(over)
    return base


def test_telemetry_requires_physical_fresh_unambiguous_samples():
    nodes = fz.gate_nodes([node("w1", "10.0.0.1"), node("w2", "10.0.0.2")], WORKERS)[1]
    now = 1_000_000.0
    top = "w1 311m 7% 3555Mi 44%\nw2 231m 5% 4184Mi 52%\n"
    series = list(fz.REQUIRED_SERIES)
    disk = fz.root_disk(disk_for(now), nodes)
    assert disk["w1"]["used_pct"] == 75.0 and fz.gate_telemetry(0, top, disk, WORKERS, series, now) == []
    assert len(fz.gate_telemetry(1, "", disk, WORKERS, series, now)) == 3
    for over in ({"size": -5.0}, {"avail": 150.0}, {"avail_ts": now + 600}, {"size_ts": now - 900}, {"size": None}):
        assert len(fz.gate_telemetry(0, top, fz.root_disk(disk_for(now, **over), nodes), WORKERS, series, now)) == 1, over
    samples, conflicts = fz.disk_samples([("size", [{"metric": {"instance": "10.0.0.1:9100"}, "value": [0, "100"]},
                                                    {"metric": {"instance": "10.0.0.1:9100"}, "value": [0, "90"]}])])
    assert conflicts == ["10.0.0.1 size"] and samples["10.0.0.1"]["size"] is None
    assert fz.gate_telemetry(0, top, disk, WORKERS, [], now)[-2:] == [f"series {s} absent" for s in fz.REQUIRED_SERIES]


# ------------------------------------------------------------------ other gates
def test_arms_gate_rejects_duplicate_autoscalers_and_paused_keda():
    deploys = [{"metadata": {"name": n}} for a in fz.ARMS for n in (a, "k6-" + a)]
    pas = [{"spec": {"targetDeployment": {"name": a}, "minReplicas": 1, "maxReplicas": 12}}
           for a in fz.ARMS if a != "myapptwo"]
    scaled = [{"metadata": {"name": "myapptwo-keda-fallback", "annotations": {}},
               "spec": {"minReplicaCount": 1, "maxReplicaCount": 12}},
              {"metadata": {"name": "nginx-test-keda-fallback", "annotations": {"autoscaling.keda.sh/paused": "true"}},
               "spec": {}}]
    assert fz.gate_arms(deploys, pas, scaled) == []
    assert any("expected one each" in p for p in fz.gate_arms(deploys, pas + [copy.deepcopy(pas[0])], scaled))
    bad = copy.deepcopy(scaled)
    bad[0]["metadata"]["annotations"]["autoscaling.keda.sh/paused"] = "true"
    bad[1]["metadata"]["annotations"] = {}
    assert len(fz.gate_arms(deploys[2:], pas, bad)) == 4


def test_requests_count_sidecars_init_containers_and_overhead():
    assert fz.quantity("250m") == 0.25 and fz.quantity("1Gi") == 2 ** 30 and fz.quantity("2") == 2.0
    with pytest.raises(ValueError):
        fz.quantity("3Xi")
    spec = {"containers": [container("a", "i", requests={"cpu": "100m"}), container("b", "i", requests={"cpu": "50m"})],
            "initContainers": [container("init", "i", requests={"cpu": "500m"}),
                               container("side", "i", requests={"cpu": "20m"}, restartPolicy="Always"),
                               container("late-init", "i", requests={"cpu": "300m"})],
            "overhead": {"cpu": "10m"}}
    assert fz.pod_requests(spec, "cpu") == pytest.approx(0.5 + 0.01)
    spec["initContainers"][0]["resources"]["requests"]["cpu"] = "100m"
    assert fz.pod_requests(spec, "cpu") == pytest.approx(0.32 + 0.01)


def test_configuration_fingerprint_and_identity():
    d = {"metadata": {"name": "nginx-test", "labels": {"a": "b"}, "annotations": {}},
         "spec": {"replicas": 2, "template": {"x": 1}}, "status": {"replicas": 2}}
    live = {("demo", "deployments.apps"): [d], ("demo", "jobs.batch"): [{"metadata": {"name": "j"}}]}
    scaled = copy.deepcopy(live)
    scaled[("demo", "deployments.apps")][0]["spec"]["replicas"] = 7
    assert fz.fingerprint_changes(fz.config_fingerprint(live), fz.config_fingerprint(scaled)) == []
    edited = copy.deepcopy(live)
    edited[("demo", "deployments.apps")][0]["spec"]["template"]["x"] = 2
    assert fz.fingerprint_changes(fz.config_fingerprint(live), fz.config_fingerprint(edited)) == \
        ["demo/deployments.apps/nginx-test: changed"]
    ident = {"head": "h", "files_sha256": {"f": "1"}}
    env = dict(ENV, HARBOR_REGISTRY="r")
    vm_s = fz.vm_static_identity([{"args": ["-retentionPeriod=30d"]}], {"vmcluster": {"spec": {"a": 1}}})
    place = fz.placement_identity([{"name": "w1", "labels": {"predictive-bench/ml-node": "true"}, "taints": []}])
    ci = lambda fp, vm=vm_s, pl=place: fz.configuration_identity(ident, {"demo": {"sha256": "s"}}, fp, env, vm, pl)
    a, b, c = ci(fz.config_fingerprint(live)), ci(fz.config_fingerprint(scaled)), ci(fz.config_fingerprint(edited))
    assert a == b and a["sha256"] != c["sha256"]
    vm2 = fz.vm_static_identity([{"args": ["-retentionPeriod=7d"]}], {"vmcluster": {"spec": {"a": 1}}})
    pl2 = fz.placement_identity([{"name": "w1", "labels": {}, "taints": []}])
    assert ci(fz.config_fingerprint(live), vm=vm2)["sha256"] != a["sha256"]                  # r55: VM settings
    assert ci(fz.config_fingerprint(live), pl=pl2)["sha256"] != a["sha256"]                  # r55: placement labels
    assert fz.compare_identity({"configuration_identity": a}, b) == []
    assert "config" in fz.compare_identity({"configuration_identity": a}, c)[0]


def package(tmp_path, name, verdict="PASS", identity=None, gates=None, complete_identity=None, version=None):
    identity = identity or fz.configuration_identity({"head": "h", "files_sha256": {}}, {}, {}, ENV, {}, {})
    gates = gates if gates is not None else {g: {"status": "PASS", "problems": []} for g in fz.MANDATORY_GATES}
    st = fz.Staging(str(tmp_path / name))
    st.json("objects/x.json", {"a": 1})
    st.json("capture.json", {"freeze_capture": version or fz.VERSION, "verdict": verdict, "gates": gates,
                             "configuration_identity": identity})
    st.publish({"verdict": verdict, "capture_sha256": fz.sha(os.path.join(st.dir, "capture.json")),
                "configuration_identity": complete_identity or identity["sha256"]})
    return str(tmp_path / name), identity


def test_r55_compare_with_verifies_the_earlier_package_end_to_end(tmp_path):
    d, ident = package(tmp_path, "good")
    cap, problems = fz.verify_capture(d)
    assert problems == [] and cap["configuration_identity"]["sha256"] == ident["sha256"]
    d2, _ = package(tmp_path, "failed", verdict="FAIL")
    assert any("diagnostic comparison only" in p for p in fz.verify_capture(d2)[1])
    d3, _ = package(tmp_path, "tampered")
    open(os.path.join(d3, "objects/x.json"), "a").write(" ")
    assert any("does not match its hash" in p for p in fz.verify_capture(d3)[1])
    d4, _ = package(tmp_path, "extra")
    open(os.path.join(d4, "objects/injected.json"), "w").write("{}")
    assert any("differ from SHA256SUMS" in p for p in fz.verify_capture(d4)[1])
    forged = dict(ident, sha256="0" * 64)
    d5, _ = package(tmp_path, "forged", identity=forged)
    assert any("does not match its body" in p for p in fz.verify_capture(d5)[1])
    assert fz.verify_capture(str(tmp_path / "missing"))[1]


def test_r56_a_pass_label_is_not_enough_for_a_baseline(tmp_path):
    ok = {g: {"status": "PASS", "problems": []} for g in fz.MANDATORY_GATES}
    failed_gate = dict(ok, runtime={"status": "FAIL", "problems": ["x"]})
    d1, _ = package(tmp_path, "pass-with-failed-gate", gates=failed_gate)
    assert any("baseline gate runtime is FAIL" in p for p in fz.verify_capture(d1)[1])
    d2, _ = package(tmp_path, "missing-gate", gates={k: v for k, v in ok.items() if k != "telemetry"})
    assert any("baseline gate telemetry is missing" in p for p in fz.verify_capture(d2)[1])
    d3, _ = package(tmp_path, "complete-identity", complete_identity="f" * 64)
    assert any("COMPLETE.json names another" in p for p in fz.verify_capture(d3)[1])
    d4, _ = package(tmp_path, "old-version", version="freeze_capture.py v4")
    assert any("unsupported capture version" in p for p in fz.verify_capture(d4)[1])
    d5, _ = package(tmp_path, "pass-but-empty-problems", gates=dict(ok, mask={"status": "PASS", "problems": ["y"]}))
    assert any("baseline gate mask" in p for p in fz.verify_capture(d5)[1])
    d6, _ = package(tmp_path, "malformed-gate", gates=dict(ok, nodes="PASS"))          # r57: shapes, not exceptions
    assert any("baseline gate nodes is malformed" in p for p in fz.verify_capture(d6)[1])
    d7, _ = package(tmp_path, "malformed-identity", identity=["not", "a", "dict"], complete_identity="x")
    assert any("does not match its body" in p for p in fz.verify_capture(d7)[1])


def test_r56_sha256sums_is_parsed_strictly(tmp_path):
    def resum(d, text):                       # rewrite SHA256SUMS and re-seal COMPLETE so only the parsing is tested
        open(os.path.join(d, "SHA256SUMS"), "w").write(text)
        c = json.load(open(os.path.join(d, "COMPLETE.json")))
        c["sha256sums_sha256"] = fz.sha(os.path.join(d, "SHA256SUMS"))
        json.dump(c, open(os.path.join(d, "COMPLETE.json"), "w"))
    d, _ = package(tmp_path, "dup")
    lines = open(os.path.join(d, "SHA256SUMS")).read()
    resum(d, lines + lines.splitlines()[0] + "\n")
    assert any("twice" in p for p in fz.verify_capture(d)[1])
    d, _ = package(tmp_path, "malformed")
    resum(d, open(os.path.join(d, "SHA256SUMS")).read() + "nonsense line\n")
    assert any("malformed" in p for p in fz.verify_capture(d)[1])
    d, _ = package(tmp_path, "link")
    target = os.path.join(d, "objects/x.json")
    real = open(target).read()
    os.rename(target, os.path.join(str(tmp_path), "outside.json"))
    open(os.path.join(str(tmp_path), "outside.json"), "w").write(real)
    os.symlink(os.path.join(str(tmp_path), "outside.json"), target)
    assert any("not a regular file" in p for p in fz.verify_capture(d)[1])
    d, _ = package(tmp_path, "badjson")
    open(os.path.join(d, "COMPLETE.json"), "w").write("{not json")
    assert "not a readable capture package" in fz.verify_capture(d)[1][0]


def test_p8_code_configmap_must_hold_the_current_code(tmp_path):
    for f in fz.P8_FILES:
        (tmp_path / f).write_text(f"content of {f}\n")
    name = "p8-code-" + fz.p8_code_id(str(tmp_path))
    good = {"data": {f: f"content of {f}\n" for f in fz.P8_FILES}, "immutable": True}
    assert fz.gate_p8_code({}, str(tmp_path)) == ([], name, False)
    assert fz.gate_p8_code({("ml-engine", name): good}, str(tmp_path))[0] == []
    bad = {"data": dict(good["data"], **{"p8_launcher.py": "other"}), "immutable": False}
    assert len(fz.gate_p8_code({("ml-engine", name): bad}, str(tmp_path))[0]) == 2


def k6_pod(arm, name=None, restarts=0, started="2026-10-07T20:30:40Z", phase="Running"):
    return {"metadata": {"name": name or f"k6-{arm}-abcdef1234-xyz12", "namespace": "demo"},
            "status": {"phase": phase, "containerStatuses": [{"name": "k6", "restartCount": restarts,
                                                              "state": {"running": {"startedAt": started}}}]}}


def test_d1097_qualification_generators_must_be_the_live_ones():
    t1, t2 = fz.hour_tag(H1), fz.hour_tag(H2)
    files = [(f"attempts/{t1}-20261008T041500Z-1.json", attempt(H1)), (f"attempts/{t2}-20261008T051500Z-1.json", attempt(H2))]
    _, qual = fz.gate_qualification(fetch_output(files, [H1, H2]), [H1, H2], EXPECT)
    pods = [k6_pod(a) for a in fz.ARMS]
    assert fz.gate_generators(qual, [H1, H2], pods) == ([], {a: [f"k6-{a}-abcdef1234-xyz12"] for a in fz.ARMS})
    replaced = [k6_pod(a, name=f"k6-{a}-newrs00000-new12") if a == "nginx-test" else k6_pod(a) for a in fz.ARMS]
    assert any("not running now" in p for p in fz.gate_generators(qual, [H1, H2], replaced)[0])
    restarted = [k6_pod(a, restarts=1) if a == "myapptwo" else k6_pod(a) for a in fz.ARMS]
    assert any("restarts 1" in p for p in fz.gate_generators(qual, [H1, H2], restarted)[0])
    late = [k6_pod(a, started="2026-10-07T21:30:00Z") if a == "nginx-seasonal" else k6_pod(a) for a in fz.ARMS]
    assert any("started 2026-10-07T21:30:00Z" in p for p in fz.gate_generators(qual, [H1, H2], late)[0])
    two = [(f"attempts/{t1}-20261008T041500Z-1.json", attempt(H1)),
           (f"attempts/{t2}-20261008T051500Z-1.json", attempt(H2, gen={"nginx-ensemble": "k6-nginx-ensemble-other00-abc12"}))]
    _, qual2 = fz.gate_qualification(fetch_output(two, [H1, H2]), [H1, H2], EXPECT)
    assert any("nginx-ensemble: qualification rows name generator pods" in p for p in fz.gate_generators(qual2, [H1, H2], pods)[0])
    assert fz.gate_generators({}, [], pods)[0] == ["no qualification evidence to bind the generators to"]


NOW_CAP = calendar.timegm((2026, 10, 12, 22, 0, 0))
FC = fz.load_forecasts()


def iso_t(t):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def issuance(n, app, art, cut, issued="2026-10-12T21:55:05Z", ns="demo", steps=6, **over):
    origin = calendar.timegm((2026, 10, 12, 21, 50, 0))
    trained = "2026-10-12T18:00:00" if app in ("nginx-ensemble", "nginx-ensemble-q95") else "2026-10-12T18:02:32Z"
    r = {"issuance_id": f"i{n}", "issued_at": issued, "application": app, "namespace": ns,
         "inference_input_end": iso_t(origin), "target_anchor": "inference_input_end", "model_version": f"{app}@m",
         "model_trained_at": trained, "training_cutoff": cut, "artifact_sha256": art, "step_minutes": 10,
         "forecasts": [[k, iso_t(origin + 600 * k), 100.0] for k in range(1, steps + 1)]}
    r.update(over)
    return r


def issuance_extract(records, receipt_over=None, tamper=False):
    rec = {"receipt": dict({"extractor": "extract_decisions.sh v3", "extractor_sha256": "e" * 64, "sha256": "f" * 64,
                            "lines": 100, "malformed_lines": 0, "issuance_fields": FC.ISSUANCE_FIELDS,
                            "issuances": len(records),
                            "source": {"context": "prodcluster", "pvc": "forecast-log-pvc", "pod": "decisions-extract-x"}},
                           **(receipt_over or {}))}
    rows = [json.dumps(["I"] + [r.get(f) for f in FC.ISSUANCE_FIELDS]) for r in records]
    body = "\n".join([json.dumps(rec)] + rows) + "\n"
    trailer = {"trailer": {"bytes": len(body.encode()), "sha256": fz.sha_bytes(body.encode())}}
    if tamper:
        body = body.replace("nginx-test", "nginx-tesT", 1)
    return (body + json.dumps(trailer) + "\n").encode()


def test_r67_fresh_issuances_must_be_accepted_demo_records_with_the_current_artifact():
    art, cut = "a" * 64, "2026-10-12T18:00:00Z"
    good = [issuance(n, a, art if a in ("nginx-test", "nginx-seasonal") else "g" * 64, cut)
            for n, a in enumerate(fz.FORECASTING_ARMS)]
    gate = lambda recs, log="", **kw: fz.gate_fresh_issuances(issuance_extract(recs, **kw), log, art, cut, NOW_CAP,
                                                             "prodcluster", "e" * 64, FC)[0]
    assert gate(good) == []
    assert fz.gate_fresh_issuances(b"", "", art, cut, NOW_CAP, "prodcluster", "e" * 64, FC)[0] == \
        ["no issuance extract given (--issuances)"]
    assert any("invalid" in p for p in gate(good, tamper=True))
    assert any("frozen extractor" in p for p in gate(good, receipt_over={"extractor_sha256": "0" * 64}))
    assert any("invalid" in p for p in gate(good, receipt_over={"issuances": 99}))          # r67: count must match
    assert any("invalid" in p for p in gate(good, receipt_over={"source": {"context": "prodcluster"}}))
    # r67 probes: null provenance / no forecasts, and complete forecasts from another namespace
    nulls = [dict(r, model_version=None, forecasts=None) if r["application"] == "nginx-test" else r for r in good]
    out = gate(nulls)
    assert any("nginx-test" in p and "P9 acceptance" in p for p in out) and any("nginx-test: no accepted" in p for p in out)
    foreign = [dict(r, namespace="other") if r["application"] == "nginx-seasonal" else r for r in good]
    assert any("nginx-seasonal: issuances outside namespace demo" in p for p in gate(foreign))
    short = [issuance(9, "nginx-ensemble", "g" * 64, cut, steps=5) if r["application"] == "nginx-ensemble" else r
             for r in good]
    assert any("nginx-ensemble" in p and "steps" in p for p in gate(short))
    stale = [issuance(9, "nginx-ensemble", "g" * 64, cut, issued="2026-10-12T21:30:00Z") if r["application"] ==
             "nginx-ensemble" else r for r in good]
    assert any("nginx-ensemble: no accepted issuance" in p for p in gate(stale))
    old_art = [dict(r, artifact_sha256="b" * 64) if r["application"] == "nginx-seasonal" else r for r in good]
    assert any("nginx-seasonal: newest issuance carries bbbbbbbbbbbb" in p for p in gate(old_art))
    log = ('2026-10-12T21:59:04Z\tINFO\tcontrollers.PredictiveAutoscaler\tPrediction unavailable, using reactive only\t'
           '{"predictiveautoscaler": {"name":"nginx-test-autoscaler","namespace":"demo"}}\n')
    assert any("nginx-test: 1 'Prediction unavailable'" in p for p in gate(good, log))
