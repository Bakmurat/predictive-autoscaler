"""The drain interval of a revocation (transition_drain.py): read from the API servers' own request limit, or
unverified. Pure functions over pod objects."""
import copy

import pytest

import transition_drain as D

IMAGE = "rancher/hardened-kubernetes:v1.35.9-rke2r1-build20261101"
DIGEST_A, DIGEST_B = "sha256:" + "a" * 64, "sha256:" + "b" * 64
AT = "2026-11-17T00:30:00Z"
ATTESTED = {DIGEST_A: {"seconds": 60, "version": "v1.35.9+rke2r1",
                       "reference": "kube-apiserver v1.35.9 options: --request-timeout default 1m0s"}}


def nodes(*names):
    return [{"name": n, "uid": f"node-uid-{n}"} for n in names]


def pod(node="cp-1", *, command=None, args=None, uid=None, image=IMAGE, phase="Running", ready=True, name=None,
        container="kube-apiserver", digest=DIGEST_A, container_id=None, restarts=0,
        started="2026-11-13T02:10:00Z", state=None, status_extra=None, **meta):
    spec_container = {"name": container, "image": image,
                      "command": ["kube-apiserver", "--secure-port=6443"] if command is None else command}
    if args is not None:
        spec_container["args"] = args
    cstatus = {"name": container, "ready": ready, "restartCount": restarts,
               "imageID": f"docker.io/rancher/hardened-kubernetes@{digest}" if digest else None,
               "containerID": container_id or f"containerd://cid-{node}",
               "state": {"running": {"startedAt": started}} if state is None else state}
    cstatus.update(status_extra or {})
    return {"metadata": dict({"name": name or f"kube-apiserver-{node}", "namespace": "kube-system",
                              "uid": uid or f"uid-{node}"}, **meta),
            "spec": {"nodeName": node, "containers": [spec_container]},
            "status": {"phase": phase, "containerStatuses": [cstatus]}}


def with_flag(node, tokens, where="command", **kw):
    tokens = tokens if isinstance(tokens, list) else [tokens]
    if where == "command":
        return pod(node, command=["kube-apiserver", "--secure-port=6443", *tokens], **kw)
    return pod(node, args=tokens, **kw)


def judge(pods, names=("cp-1",), attested=ATTESTED):
    return D.verdict(pods, nodes(*names), attested, AT)


# --- durations -------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("text,seconds", [("1m0s", 60), ("60s", 60), ("90s", 90), ("2m", 120), ("1h", 3600),
                                          ("1m30s", 90), ("1.5m", 90), (".5m", 30), ("500ms", 0.5), ("1h1m1s", 3661),
                                          ("0h1m0s", 60), ("2m0.5s", 120.5)])
def test_go_durations_are_read(text, seconds):
    assert D.parse_duration(text) == pytest.approx(seconds)


@pytest.mark.parametrize("text", ["", "60", "1", "1min", "one minute", "1m 0s", " 1m", "1m ", "-1m", "+1m", "1m-30s",
                                  "0", "0s", "0m0s", "1e3s", "1,5m", "m", "s", "1d", "1M", "60S", "1m0", "nan", "inf",
                                  None, 60, 60.0, True, ["1m"]])
def test_anything_that_is_not_a_positive_duration_is_refused(text):
    with pytest.raises(D.Unverified):
        D.parse_duration(text)


# --- the invocation must be a direct one -----------------------------------------------------------------------------

@pytest.mark.parametrize("command,args,expected", [
    (["kube-apiserver", "--a=1"], None, ["kube-apiserver", "--a=1"]),
    (["/usr/local/bin/kube-apiserver"], ["--a=1", "--b", "2"], ["/usr/local/bin/kube-apiserver", "--a=1", "--b", "2"]),
    (["kube-apiserver"], [], ["kube-apiserver"]),
])
def test_a_direct_invocation_is_its_complete_argument_list(command, args, expected):
    container = {"command": command}
    if args is not None:
        container["args"] = args
    assert D.invocation(container) == expected


@pytest.mark.parametrize("name,command,args", [
    # The reviewer's reproduction: the flag is inside a shell string, so "no matching token" is not "flag absent".
    ("a shell wrapper", ["/bin/sh", "-c", "exec kube-apiserver --request-timeout=10m"], None),
    ("sh by name", ["sh", "-c", "kube-apiserver --request-timeout=10m"], None),
    ("env", ["/usr/bin/env", "kube-apiserver", "--request-timeout=10m"], None),
    ("another launcher", ["hyperkube", "kube-apiserver", "--request-timeout=10m"], None),
    ("a similar name", ["kube-apiserver-wrapper", "--request-timeout=10m"], None),
    ("no command at all", [], None),
    ("no command key", None, None),
    ("everything in args, entrypoint unknown", [], ["kube-apiserver", "--request-timeout=10m"]),
    ("only args", None, ["--request-timeout=10m"]),
    ("a directory", ["kube-apiserver/"], None),
    ("command is a string", "kube-apiserver --request-timeout=10m", None),
    ("a non-string token", ["kube-apiserver", 60], None),
    ("args is a string", ["kube-apiserver"], "--request-timeout=1m"),
    ("args holds a list", ["kube-apiserver"], [["--request-timeout=1m"]]),
])
def test_an_unknown_invocation_concludes_nothing_from_not_seeing_the_flag(name, command, args):
    container = {"name": "kube-apiserver", "image": IMAGE}
    if command is not None:
        container["command"] = command
    if args is not None:
        container["args"] = args
    with pytest.raises(D.Unverified):
        D.invocation(container)
    with pytest.raises(D.Unverified):
        D.request_timeout(container)
    p = pod()
    p["spec"]["containers"] = [container]
    out = judge([p])
    assert out["verified"] is False and out["seconds"] is None, name    # NOT the attested default plus the margin
    assert "kube-system/kube-apiserver-cp-1" in out["why"]


# --- the flag, in command and args, in the two accepted forms --------------------------------------------------------

@pytest.mark.parametrize("tokens,where", [
    (["--request-timeout=2m0s"], "command"), (["--request-timeout", "2m0s"], "command"),
    (["--request-timeout=2m0s"], "args"), (["--request-timeout", "120s"], "args"),
])
def test_the_flag_is_found_in_command_and_args_in_both_accepted_forms(tokens, where):
    (container,) = with_flag("cp-1", tokens, where)["spec"]["containers"]
    assert D.request_timeout(container) == 120


def test_no_flag_in_a_direct_invocation_is_none_and_similar_flags_are_not_the_flag():
    (container,) = pod(command=["kube-apiserver", "--request-timeout-extra=5s", "--min-request-timeout=1800",
                                "--shutdown-delay-duration=10s"])["spec"]["containers"]
    assert D.request_timeout(container) is None


@pytest.mark.parametrize("command,args", [
    (["kube-apiserver", "-request-timeout=2m"], None),                                 # single dash: not a pflag form
    (["kube-apiserver", "-request-timeout", "2m"], None),
    (["kube-apiserver"], ["-request-timeout=2m"]),
    (["kube-apiserver", "--request-timeout=1m", "--request-timeout=2m"], None),        # twice in command
    (["kube-apiserver", "--request-timeout=1m"], ["--request-timeout=1m"]),            # command and args, same value
    (["kube-apiserver", "--request-timeout=1m"], ["--request-timeout", "2m"]),
    (["kube-apiserver", "--request-timeout"], None),                                  # no value: the last token
    (["kube-apiserver", "--request-timeout", "--secure-port=6443"], None),            # no value: a flag follows
    (["kube-apiserver", "--request-timeout", "-30s"], None),                          # a dash-prefixed value
    (["kube-apiserver", "--request-timeout="], None),
    (["kube-apiserver", "--request-timeout=soon"], None),
    (["kube-apiserver", "--request-timeout=0s"], None),
    (["kube-apiserver", "--request-timeout=-30s"], None),
    (["kube-apiserver", "--request-timeout=60"], None),                               # a number without a unit
])
def test_a_flag_in_an_unsupported_repeated_valueless_or_malformed_form_is_refused(command, args):
    container = {"name": "kube-apiserver", "image": IMAGE, "command": command}
    if args is not None:
        container["args"] = args
    with pytest.raises(D.Unverified):
        D.request_timeout(container)
    p = pod()
    p["spec"]["containers"] = [container]
    out = judge([p])
    assert out["verified"] is False and out["seconds"] is None


# --- the verdict -----------------------------------------------------------------------------------------------------

def test_an_explicit_value_on_every_server_is_the_limit_plus_the_margin():
    pods = [with_flag("cp-1", "--request-timeout=2m0s"), with_flag("cp-2", ["--request-timeout", "120s"], "args"),
            with_flag("cp-3", "--request-timeout=2m", digest=DIGEST_B)]
    out = judge(pods, ("cp-1", "cp-2", "cp-3"), attested={})                # no default is needed
    assert out["verified"] is True and out["request_timeout"] == 120 and out["seconds"] == 150
    assert out["nodes"] == nodes("cp-1", "cp-2", "cp-3") and out["observed_at"] == AT
    assert [s["node"] for s in out["servers"]] == ["cp-1", "cp-2", "cp-3"]
    assert out["servers"][0] == {
        "node": "cp-1", "pod": "kube-system/kube-apiserver-cp-1", "uid": "uid-cp-1", "image": IMAGE,
        "image_digest": DIGEST_A, "container_id": "containerd://cid-cp-1", "restart_count": 0,
        "started_at": "2026-11-13T02:10:00Z",
        "invocation": ["kube-apiserver", "--secure-port=6443", "--request-timeout=2m0s"], "seconds": 120,
        "default": None}
    assert all(set(s) == set(D.SERVER_FIELDS) for s in out["servers"])
    assert D.MARGIN_SECONDS == 30 and out["why"] == "request timeout 120 s on 3 API server(s), plus 30 s"


def test_absent_everywhere_uses_the_default_attested_for_each_servers_image_digest():
    pods = [pod("cp-1"), pod("cp-2")]
    out = judge(pods, ("cp-1", "cp-2"))
    assert out["verified"] is True and out["request_timeout"] == 60 and out["seconds"] == 90
    for s in out["servers"]:
        assert s["seconds"] is None                                        # recorded as ABSENT, not as 60
        assert s["default"] == {"seconds": 60.0, "version": "v1.35.9+rke2r1",
                                "reference": ATTESTED[DIGEST_A]["reference"]}
    # Another binary (another digest) under the SAME tag is not covered by the attestation.
    other = [pod("cp-1"), pod("cp-2", digest=DIGEST_B)]
    refused = judge(other, ("cp-1", "cp-2"))
    assert refused["verified"] is False and DIGEST_B in refused["why"]
    both = dict(ATTESTED, **{DIGEST_B: dict(ATTESTED[DIGEST_A], version="v1.35.10+rke2r1")})
    assert judge(other, ("cp-1", "cp-2"), attested=both)["verified"] is True
    differing = dict(ATTESTED, **{DIGEST_B: dict(ATTESTED[DIGEST_A], seconds=30)})
    assert judge(other, ("cp-1", "cp-2"), attested=differing)["verified"] is False


@pytest.mark.parametrize("attested", [
    {}, None, [60], {"1.35": 60}, {DIGEST_A: 60}, {DIGEST_A: None},
    {DIGEST_A: {"seconds": 60}}, {DIGEST_A: {"seconds": 60, "version": "v1.35.9"}},
    {DIGEST_A: {"seconds": 60, "reference": "x"}}, {DIGEST_A: {"seconds": 60, "version": "", "reference": "x"}},
    {DIGEST_A: {"seconds": 60, "version": "v1.35.9", "reference": " "}},
    {DIGEST_A: {"seconds": 0, "version": "v", "reference": "r"}},
    {DIGEST_A: {"seconds": -60, "version": "v", "reference": "r"}},
    {DIGEST_A: {"seconds": "60", "version": "v", "reference": "r"}},
    {DIGEST_A: {"seconds": True, "version": "v", "reference": "r"}},
    {DIGEST_A: {"seconds": float("nan"), "version": "v", "reference": "r"}},
    {DIGEST_A.upper(): ATTESTED[DIGEST_A]}, {IMAGE: ATTESTED[DIGEST_A]},
])
def test_a_default_is_used_only_with_a_complete_attestation_for_that_digest(attested):
    out = judge([pod("cp-1")], attested=attested)
    assert out["verified"] is False and out["seconds"] is None, attested


@pytest.mark.parametrize("pods,why", [
    ([with_flag("cp-1", "--request-timeout=1m"), with_flag("cp-2", "--request-timeout=2m")], "different"),
    ([with_flag("cp-1", "--request-timeout=1m"), pod("cp-2")], "some do not"),
    ([pod("cp-1"), with_flag("cp-2", "--request-timeout=60s")], "some do not"),          # even when it equals the default
])
def test_servers_that_do_not_agree_are_unverified(pods, why):
    out = judge(pods, ("cp-1", "cp-2"))
    assert out["verified"] is False and out["seconds"] is None and why in out["why"]


@pytest.mark.parametrize("name,pods,inventory", [
    ("no pod selected", [], nodes("cp-1")),
    ("pods is not a list", None, nodes("cp-1")),
    ("a pod is not an object", ["kube-apiserver-cp-1"], nodes("cp-1")),
    ("a control-plane node without a pod", [pod("cp-1")], nodes("cp-1", "cp-2")),
    ("a pod outside the inventory", [pod("cp-1"), pod("worker-9")], nodes("cp-1")),
    ("only a pod outside the inventory", [pod("worker-9")], nodes("cp-1")),
    ("two pods on one node", [pod("cp-1"), pod("cp-1", uid="uid-b", name="kube-apiserver-cp-1-b")], nodes("cp-1")),
])
def test_an_incomplete_or_mismatching_selection_proves_nothing(name, pods, inventory):
    out = D.verdict(pods, inventory, ATTESTED, AT)
    assert out["verified"] is False and out["seconds"] is None and out["why"], name


@pytest.mark.parametrize("inventory", [
    [], None, "cp-1", ["cp-1"], [{"name": "cp-1"}], [{"uid": "u"}], [{"name": "cp-1", "uid": ""}],
    [{"name": "", "uid": "u"}], [{"name": "cp-1", "uid": "u", "role": "x"}],
    [{"name": "cp-1", "uid": "u"}, {"name": "cp-1", "uid": "v"}],          # a repeated name
    [{"name": "cp-1", "uid": "u"}, {"name": "cp-2", "uid": "u"}],          # a repeated UID
    [None], {},
])
def test_an_empty_or_malformed_inventory_is_named_as_the_reason(inventory):
    out = D.verdict([pod("cp-1")], inventory, ATTESTED, AT)
    assert out["verified"] is False and "inventory is empty or malformed" in out["why"], inventory
    assert out["servers"] == [] and out["nodes"] == []                     # nothing was even looked at


@pytest.mark.parametrize("name,broken", [
    ("pending", pod(phase="Pending")), ("failed", pod(phase="Failed")), ("not ready", pod(ready=False)),
    ("being deleted", pod(deletionTimestamp="2026-11-17T00:00:00Z")),
    ("no uid", {**pod(), "metadata": {"name": "kube-apiserver-cp-1", "namespace": "kube-system"}}),
    ("no node", {**pod(), "spec": {"containers": pod()["spec"]["containers"]}}),
    ("another container name", pod(container="apiserver")),
    ("no image", pod(image=None)),
    ("no status", {k: v for k, v in pod().items() if k != "status"}),
    ("waiting, not running", pod(state={"waiting": {"reason": "CrashLoopBackOff"}})),
    ("terminated", pod(state={"terminated": {"exitCode": 1}})),
    ("running without a start time", pod(state={"running": {}})),
    ("state is not an object", pod(state="running")),
    ("no image digest", pod(digest=None)),
    ("an imageID without a digest", pod(status_extra={"imageID": "docker.io/rancher/hardened-kubernetes:v1.35.9"})),
    ("no container ID", pod(status_extra={"containerID": ""})),
    ("no restart count", pod(status_extra={"restartCount": None})),
    ("a negative restart count", pod(restarts=-1)),
    ("a boolean restart count", pod(status_extra={"restartCount": True})),
])
def test_a_server_without_a_running_identified_container_is_unverified(name, broken):
    out = judge([broken])
    assert out["verified"] is False and out["seconds"] is None, name
    two = judge([pod("cp-2"), broken], ("cp-1", "cp-2"))                     # one bad server spoils the whole set
    assert two["verified"] is False, name


def test_two_apiserver_containers_or_two_statuses_in_one_pod_are_refused():
    p = pod()
    p["spec"]["containers"].append(copy.deepcopy(p["spec"]["containers"][0]))
    assert judge([p])["verified"] is False
    q = pod()
    q["status"]["containerStatuses"].append(copy.deepcopy(q["status"]["containerStatuses"][0]))
    assert judge([q])["verified"] is False


@pytest.mark.parametrize("at", ["", None, " ", 1763339400, ["2026-11-17T00:30:00Z"]])
def test_the_observation_time_is_required(at):
    out = D.verdict([pod("cp-1")], nodes("cp-1"), ATTESTED, at)
    assert out["verified"] is False and "observation time" in out["why"]


# --- the plan's servers against the live ones ----------------------------------------------------------------------

def planned_two():
    return judge([pod("cp-1"), pod("cp-2")], ("cp-1", "cp-2"))


def test_the_same_servers_seen_later_are_the_same():
    planned = planned_two()
    later = D.verdict([pod("cp-2"), pod("cp-1")], nodes("cp-2", "cp-1"), ATTESTED, "2026-11-17T21:50:00Z")
    assert later["verified"] and later["observed_at"] != planned["observed_at"]
    assert D.same_servers(planned, later)                                 # order and observation time do not matter


@pytest.mark.parametrize("name,change", [
    # The reviewer's reproduction: the container was replaced INSIDE the same pod (same pod UID, same image tag).
    ("another image digest", dict(digest=DIGEST_B)),
    ("another container ID", dict(container_id="containerd://another")),
    ("a restart", dict(restarts=1)),
    ("another start time", dict(started="2026-11-17T01:00:00Z")),
    ("a replaced pod (new UID)", dict(uid="uid-new")),
    ("another image tag", dict(image=IMAGE.replace("1.35.9", "1.35.10"))),
    ("another pod name", dict(name="kube-apiserver-cp-1-x")),
    ("another invocation", dict(command=["kube-apiserver", "--secure-port=6443", "--v=4"])),
    ("an executable path", dict(command=["/usr/bin/kube-apiserver", "--secure-port=6443"])),
])
def test_a_restarted_replaced_or_reconfigured_server_is_another_server(name, change):
    planned = planned_two()
    attested = dict(ATTESTED, **{DIGEST_B: ATTESTED[DIGEST_A]})
    live = D.verdict([pod("cp-1", **change), pod("cp-2")], nodes("cp-1", "cp-2"), attested, AT)
    assert live["verified"] is True, (name, live["why"])                  # still a valid verdict on its own...
    assert live["seconds"] == planned["seconds"]
    assert not D.same_servers(planned, live), name                        # ...but not the servers the plan recorded


def test_other_changes_since_the_plan_are_seen_too():
    planned = planned_two()
    cases = {
        "a flag that appeared": D.verdict([with_flag("cp-1", "--request-timeout=1m0s"),
                                           with_flag("cp-2", "--request-timeout=60s")], nodes("cp-1", "cp-2"), {}, AT),
        "a node that was replaced (new UID)": D.verdict([pod("cp-1"), pod("cp-2")],
                                                        [{"name": "cp-1", "uid": "another"},
                                                         {"name": "cp-2", "uid": "node-uid-cp-2"}], ATTESTED, AT),
        "another attestation": D.verdict([pod("cp-1"), pod("cp-2")], nodes("cp-1", "cp-2"),
                                         {DIGEST_A: dict(ATTESTED[DIGEST_A], reference="another source")}, AT),
        "a server that is gone": judge([pod("cp-1")], ("cp-1",)),
        "a server that is not ready": judge([pod("cp-1", ready=False), pod("cp-2")], ("cp-1", "cp-2")),
    }
    for name, live in cases.items():
        assert not D.same_servers(planned, live), name
    assert cases["a flag that appeared"]["seconds"] == planned["seconds"]   # the same interval is not enough


def test_same_servers_needs_both_verdicts_verified_and_whole_whatever_else_they_say():
    good = planned_two()
    assert D.same_servers(good, copy.deepcopy(good))
    for flag in (False, None, "true", 1):
        claimed = dict(copy.deepcopy(good), verified=flag)
        assert not D.same_servers(claimed, good) and not D.same_servers(good, claimed), flag
        assert not D.same_servers(claimed, copy.deepcopy(claimed)), flag
    unverified = judge([pod("cp-1")], ("cp-1", "cp-2"))
    assert not D.same_servers(unverified, unverified)
    assert not D.same_servers(good, None) and not D.same_servers(None, good) and not D.same_servers({}, {})
    for strip in ("container_id", "started_at", "image_digest", "restart_count", "invocation"):
        cut = copy.deepcopy(good)
        for s in cut["servers"]:
            del s[strip]
        assert not D.same_servers(cut, copy.deepcopy(cut)), strip          # a record without an identity field
    for key in ("nodes", "servers"):
        empty = dict(copy.deepcopy(good), **{key: []})
        assert not D.same_servers(empty, copy.deepcopy(empty)), key
    assert not D.same_servers(dict(copy.deepcopy(good), seconds=None), dict(copy.deepcopy(good), seconds=None))
    assert not D.same_servers(dict(copy.deepcopy(good), request_timeout=30), good)


# --- second code review: Kubernetes expands $(VAR) in command and args ----------------------------------------------

@pytest.mark.parametrize("name,command,args", [
    # The reviewer's reproduction: the whole flag comes from the environment (TIMEOUT_FLAG=--request-timeout=10m).
    ("the whole flag in command", ["kube-apiserver", "$(TIMEOUT_FLAG)"], None),
    ("the whole flag in args", ["kube-apiserver"], ["$(TIMEOUT_FLAG)"]),
    ("a variable inside the flag's name", ["kube-apiserver", "--request-$(WHAT)=10m"], None),
    ("the flag's name from a variable", ["kube-apiserver"], ["--$(NAME)=10m"]),
    ("a prefix that may hold the flag", ["kube-apiserver", "$(EXTRA)--secure-port=6443"], None),
    ("the value from a variable", ["kube-apiserver", "--request-timeout=$(TIMEOUT)"], None),
    ("the spaced value from a variable", ["kube-apiserver"], ["--request-timeout", "$(TIMEOUT)"]),
    ("the executable's directory from a variable", ["$(BIN_DIR)/kube-apiserver", "--secure-port=6443"], None),
    ("a reference in another flag's value", ["kube-apiserver", "--advertise-address=$(POD_IP)"], None),
    ("the escaped form", ["kube-apiserver", "$$(TIMEOUT_FLAG)"], None),
    ("an unterminated reference", ["kube-apiserver", "--request-$(WHAT=10m"], None),
])
def test_an_argument_with_a_variable_reference_makes_the_invocation_unknown(name, command, args):
    container = {"name": "kube-apiserver", "image": IMAGE, "command": command,
                 "env": [{"name": "TIMEOUT_FLAG", "value": "--request-timeout=10m"}, {"name": "TIMEOUT", "value": "10m"}]}
    if args is not None:
        container["args"] = args
    with pytest.raises(D.Unverified, match="variable reference|not started directly"):
        D.invocation(container)
    with pytest.raises(D.Unverified):
        D.request_timeout(container)
    p = pod()
    p["spec"]["containers"] = [container]
    # Through the verdict WITH a valid default attestation: reading the flag as "absent" would verify 90 s here,
    # although the expanded argument sets 600 s.
    out = judge([p])
    assert out["verified"] is False and out["seconds"] is None and out["request_timeout"] is None, name
    two = judge([pod("cp-2"), p], ("cp-1", "cp-2"))
    assert two["verified"] is False, name


def test_a_dollar_sign_without_the_reference_syntax_is_an_ordinary_character():
    (container,) = pod(command=["kube-apiserver", "--secure-port=6443", "--audit-log-path=/var/log/$HOST/a.log",
                                "--feature-gates=A=true", "--etcd-prefix=/reg$"])["spec"]["containers"]
    assert D.request_timeout(container) is None                           # "$" alone is not expanded by Kubernetes
    assert judge([pod(command=container["command"])])["verified"] is True
