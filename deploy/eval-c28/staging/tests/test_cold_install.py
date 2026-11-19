"""A cold installation, across the three pieces that the admission run of 2026-11-19 showed apart.

That run's verdict could not pass: (1) the rendered objects had a scrape object for the ingress only, so nothing
scraped the fixture targets; (2) the ingress created its per-target counters at the first request, so the first
minute of the run had no sample before its start. Every unit test passed, because each piece was tested with series
that already existed. This test starts from nothing: the objects are RENDERED, the fixture is BUILT and started as
fresh processes with the rendered configuration, its /metrics is read the way the rendered scrape objects say (before
any request and during the traffic), and the reducer's own boundary rule judges the first minute.

What it does not do: run a scraper or a cluster. The scrape instants are assigned (one before the origin, then one per
interval), the processes listen on free local ports instead of the rendered container port, and the ingress routes
through its upstream URLs (ROUTING=service) because endpoint discovery needs the API server; the dispatch counters of
discovered endpoints are covered by the fixture's own Go tests (zero_counters_test.go).

Needs the Go toolchain ($GO or `go` on PATH). Without it the test is skipped, unless C28_REQUIRE_GO=1 (CI and the
commit gate), where a missing toolchain is a failure.
"""

import importlib
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request

import pytest

import bounded as B
import execlib as X
import prepare
from part2_fixtures import api  # noqa: F401
from test_prepare_demo import N, R, world  # noqa: F401

GO = os.environ.get("GO") or shutil.which("go")
REQUIRED = os.environ.get("C28_REQUIRE_GO") == "1"
FIXTURE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "fixture")
ORIGIN = 1_800_000_000.0                                   # a whole minute; the scrape instants are relative to it

pytestmark = pytest.mark.skipif(not GO and not REQUIRED, reason="no Go toolchain: the fixture cannot be built")


@pytest.fixture(scope="module")
def binary(tmp_path_factory):
    assert GO, "C28_REQUIRE_GO=1 but there is no Go toolchain"
    out = str(tmp_path_factory.mktemp("fixture") / "c28-fixture")
    subprocess.run([GO, "build", "-o", out, "."], cwd=FIXTURE_DIR, check=True, timeout=600,
                   env={**os.environ, "CGO_ENABLED": "0"})
    return out


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get(url: str, timeout: float = 5.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read().decode()


def _exposition(text: str) -> dict:
    """{(metric name, frozenset of label pairs): value} of a Prometheus text exposition (no escapes in these labels)."""
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        head, value = line.rsplit(" ", 1)
        name, _, rest = head.partition("{")
        labels = frozenset(tuple(pair.split("=", 1)) for pair in rest.rstrip("}").split(",")) if rest else frozenset()
        out[(name, frozenset((k, v.strip('"')) for k, v in labels))] = float(value)
    return out


def _value(expo: dict, name: str, **labels):
    """The one series of that name carrying those labels (None when absent)."""
    hits = [v for (n, ls), v in expo.items() if n == name and set(labels.items()) <= set(ls)]
    assert len(hits) <= 1, (name, labels, hits)
    return hits[0] if hits else None


def _selected(scrape: dict, deployments: dict) -> list:
    sel = scrape["spec"]["selector"]

    def match(labels):
        return all(labels.get(k) == v for k, v in (sel.get("matchLabels") or {}).items()) and all(
            (e["operator"] == "In" and labels.get(e["key"]) in e["values"]) for e in sel.get("matchExpressions") or [])
    return sorted(n for n, d in deployments.items() if match(d["spec"]["template"]["metadata"]["labels"]))


def test_a_cold_installation_is_scraped_and_its_first_minute_is_observed(world, binary, tmp_path):  # noqa: F811
    record, _ = world()
    params = prepare.load_params(record, "demonstration")
    c28 = X.import_harness()
    reduce = importlib.import_module("c28.demo_reduce")
    ns = params["namespace"]
    objs = [o for part in c28.demo.render(params).values() for o in part]

    # --- 1. rendering: a scrape object for the ingress AND one for the targets; each selects exactly its pods, and its
    #        endpoint names a port that the selected container declares
    scrapes = {o["metadata"]["name"]: o for o in objs if o["kind"] == params["scrape"]}
    deployments = {o["metadata"]["name"]: o for o in objs if o["kind"] == "Deployment" and o["metadata"]["namespace"] == ns}
    assert sorted(scrapes) == ["c28-ingress", "c28-targets"]
    assert _selected(scrapes["c28-targets"], deployments) == sorted([N, R])
    assert _selected(scrapes["c28-ingress"], deployments) == ["c28-ingress"]

    def rendered(name: str, scrape: str):
        container = deployments[name]["spec"]["template"]["spec"]["containers"][0]
        ep = scrapes[scrape]["spec"]["podMetricsEndpoints"][0]
        assert ep["port"] in [x["name"] for x in container["ports"]], (name, ep)
        return {e["name"]: e["value"] for e in container["env"]}, ep["path"], int(ep["interval"].rstrip("s"))
    faults = {o["metadata"]["name"]: o["data"]["faults.json"] for o in objs
              if o["kind"] == "ConfigMap" and o["metadata"]["name"].endswith("-faults")}

    # --- 2. fresh processes with the rendered configuration (see the module text for the three local substitutions)
    procs, ports, paths = [], {}, {}
    interval = None

    def start(name: str, env: dict):
        ports[name] = _free_port()
        procs.append(subprocess.Popen([binary], env={"PATH": os.environ.get("PATH", ""), **env, "PORT": str(ports[name])},
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    try:
        for t in (N, R):
            env, paths[t], interval = rendered(t, "c28-targets")
            assert env["MODE"] == "server" and env["TARGET"] == t
            fault_file = tmp_path / f"{t}-faults.json"
            fault_file.write_text(faults[f"{t}-faults"])                      # the rendered, EMPTY fault schedule
            start(t, {**env, "STARTUP_DELAY_SECONDS": "0", "FAULT_FILE": str(fault_file)})
        env, paths["c28-ingress"], ingress_interval = rendered("c28-ingress", "c28-ingress")
        assert env["MODE"] == "ingress" and ingress_interval == interval
        assert env["UPSTREAMS"] == ",".join(f"{t}=http://{t}.{ns}.svc" for t in (N, R))
        start("c28-ingress", {**env, "ROUTING": "service",
                              "UPSTREAMS": ",".join(f"{t}=http://127.0.0.1:{ports[t]}" for t in (N, R))})
        deadline = time.time() + 20
        for name, port in ports.items():
            while True:
                try:
                    if _get(f"http://127.0.0.1:{port}/ready")[0] == 200:
                        break
                except (urllib.error.URLError, ConnectionError, OSError):
                    pass
                assert time.time() < deadline, f"{name} did not become ready"
                time.sleep(0.05)

        def scrape(name: str) -> dict:
            status, text = _get(f"http://127.0.0.1:{ports[name]}{paths[name]}")
            assert status == 200
            return _exposition(text)

        # --- 3. scraped BEFORE any request: every counter the verdict reads exists, at zero, under the labels the
        #        collector's selectors use (the scraper adds namespace and pod)
        cold = scrape("c28-ingress")
        samples = {t: [] for t in (N, R)}
        for t in (N, R):
            assert B.target_selector("c28_ingress_requests_total", ns, t) == \
                f'c28_ingress_requests_total{{namespace="{ns}",target="{t}"}}'
            assert _value(cold, "c28_ingress_requests_total", target=t) == 0.0, "the ingress counter exists before traffic"
            own = scrape(t)
            assert _value(own, "c28_requests_total", target=t, code="200") == 0.0, "the fixture counter exists before traffic"
            assert _value(own, "c28_queue", target=t) == 0.0
            samples[t].append((ORIGIN - 10.0, 0.0))

        # --- 4. traffic, scraped once per interval: 4 requests per target before each later scrape
        sent = 0
        for k in range(6):
            for t in (N, R):
                for _ in range(4):
                    assert _get(f"http://127.0.0.1:{ports['c28-ingress']}/t/{t}/work")[0] == 200
            sent += 4
            now = scrape("c28-ingress")
            for t in (N, R):
                samples[t].append((ORIGIN - 10.0 + interval * (k + 1), _value(now, "c28_ingress_requests_total", target=t)))
        for t in (N, R):
            assert samples[t][-1][1] == sent == 24
            assert _value(scrape(t), "c28_requests_total", target=t, code="200") == 24.0     # counted by the pod itself

        # --- 5. the reducer's own rule for the run's FIRST minute: observed, because a sample precedes the origin ...
        for t in (N, R):
            row = reduce._ingress_minute(samples[t], ORIGIN, ORIGIN + 60.0, reduce.DEFAULT_MAX_SCRAPE_GAP_S)
            assert row["state"] == "ok", row
            lo, hi = row["received_bounds"]
            assert lo <= 20 and hi >= 16 and 0 < row["received"] <= 24, row   # 16 by the scrape before +60 s, 20 by the next
            # ... and exactly what the installation of 2026-11-19 recorded when the series was born at the first
            # request: the same samples without the one before the origin are a gap, never a guessed zero
            born_late = reduce._ingress_minute(samples[t][1:], ORIGIN, ORIGIN + 60.0, reduce.DEFAULT_MAX_SCRAPE_GAP_S)
            assert born_late["state"] == "gap" and born_late["reasons"] == ["ingress_gap"], born_late
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
