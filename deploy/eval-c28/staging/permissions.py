#!/usr/bin/env python3
"""Read-only proof that the evaluation operator has NO replica-write permission (INSTALL-PLAN draft 2, stage A step 4;
Codex execution review Q2): the r3 shadow smoke's S1 collector and verdict, extracted for the live cluster.

  permissions.py --record R check --targets c28-demo-pa-n,c28-demo-pa-r --out F [--sa NAME] [--sa-namespace NS]

Asked of the API server's authorizer under impersonation of the operator's ServiceAccount WITH its three groups
(system:serviceaccounts, system:serviceaccounts:<ns>, system:authenticated), in the watched namespace:
  * the scale subresource (deployments.apps --subresource=scale; "deployments/scale" as one argument would be read as a
    Deployment named "scale"): update and patch must be no, get yes (the control), for the question without a name AND
    for every explicitly planned target name (the targets are given, never discovered: before the PAs exist the names
    are the planned ones);
  * direct writes: update, patch and delete on deployments.apps, create and delete on pods: all must be no;
  * the scale write without a name in EVERY namespace of the cluster and cluster-wide (-A): no, never a sample.
(Aggregated ClusterRoles are read with the rules the aggregation controller filled in, i.e. the rules in effect.
Run it for the forecaster's and the trainer's ServiceAccounts too, with --sa.)
And from the RBAC objects of the WHOLE cluster (ClusterRoles, Roles, ClusterRoleBindings, RoleBindings of every
namespace; no other kind is read): every binding whose subject is the ServiceAccount, its username or one of its groups
is resolved to its role, and any rule there that reaches the scale subresource, Deployments or pods with a write verb
is a finding, name-restricted or not.

Verdict: REFUSED (exit 1) on any yes to a write or any such grant; UNPROVEN (exit 3) on an unclear or missing answer,
an API or impersonation error, a failed control, an RBAC list that could not be read, or no target; PASS (exit 0)
otherwise. The answers, the identity's bindings, the grants and every reason are written to --out.
The negative self-test (an injected name-restricted grant) stays on kind (smoke/kind.sh S1).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Callable, Dict, List, Tuple

SCALE_VERBS = ("update", "patch", "get")
DIRECT = [("update", "deployments.apps"), ("patch", "deployments.apps"), ("delete", "deployments.apps"),
          ("create", "pods"), ("delete", "pods")]
WRITES = {"update", "patch", "create", "delete", "deletecollection", "*"}
RBAC_KINDS = ("clusterroles", "roles", "clusterrolebindings", "rolebindings")


def groups_of(sa_ns: str) -> List[str]:
    return ["system:serviceaccounts", f"system:serviceaccounts:{sa_ns}", "system:authenticated"]


def collect(kubectl: Callable[[List[str]], Tuple[int, str]], sa_ns: str, sa: str, ns: str, targets: List[str]) -> Dict:
    """Read-only questions and lists. kubectl(args) -> (exit code, stdout); its stderr is never kept (it may quote a
    request). An error is recorded as the answer "error"."""
    imp = [f"--as=system:serviceaccount:{sa_ns}:{sa}"] + [f"--as-group={g}" for g in groups_of(sa_ns)]
    answers: Dict[str, Dict[str, str]] = {}

    def ask(key, verb, resource, extra):
        rc, out = kubectl(["auth", "can-i", verb, resource, "-n", ns, *extra, *imp])
        word = out.strip().splitlines()[-1].strip() if out.strip() else ""
        # can-i exits 1 with "no" for a denied question: only the printed word counts, and only yes/no
        answers.setdefault(key, {})[verb] = word if word in ("yes", "no") else "error"
    for name in ["-"] + list(targets):
        for verb in SCALE_VERBS:
            ask(name, verb, "deployments.apps" + ("" if name == "-" else f"/{name}"), ["--subresource=scale"])
    for verb, res in DIRECT:
        ask(f"direct:{res}", verb, res, [])
    # The scale write without a name in EVERY namespace of the cluster, and cluster-wide (-A): never a sample.
    rc, out = kubectl(["get", "namespaces", "-o", "json"])
    try:
        namespaces = sorted(i["metadata"]["name"] for i in json.loads(out)["items"]) if rc == 0 else None
    except (ValueError, KeyError, TypeError):
        namespaces = None
    for other in namespaces or []:
        for verb in ("update", "patch"):
            rc_, out_ = kubectl(["auth", "can-i", verb, "deployments.apps", "--subresource=scale", "-n", other, *imp])
            word = out_.strip().splitlines()[-1].strip() if out_.strip() else ""
            answers.setdefault(f"ns:{other}", {})[verb] = word if word in ("yes", "no") else "error"
    for verb in ("update", "patch"):
        rc_, out_ = kubectl(["auth", "can-i", verb, "deployments.apps", "--subresource=scale", "-A", *imp])
        word = out_.strip().splitlines()[-1].strip() if out_.strip() else ""
        answers.setdefault("cluster", {})[verb] = word if word in ("yes", "no") else "error"
    rbac, list_errors = [], []
    for kind in RBAC_KINDS:
        rc, out = kubectl(["get", kind, "-o", "json"] + (["-A"] if kind in ("roles", "rolebindings") else []))
        try:
            doc = json.loads(out) if rc == 0 else None
        except ValueError:
            doc = None
        if not isinstance(doc, dict) or not isinstance(doc.get("items"), list):
            list_errors.append(kind)
            continue
        rbac.extend(doc["items"])
    if namespaces is None:
        list_errors.append("namespaces")
    return {"answers": answers, "rbac": rbac, "list_errors": list_errors, "namespaces": namespaces or []}


def _role_kind(o: Dict) -> str:
    return o.get("kind") or ""


def verdict(collected: Dict, sa_ns: str, sa: str, targets: List[str]) -> Dict:
    answers, refused, unproven = collected["answers"], [], []
    if not targets:
        unproven.append("no target to ask about")
    for name in ["-"] + list(targets):
        label = "of any Deployment (no name)" if name == "-" else f"of Deployment {name}"
        for verb in SCALE_VERBS:
            got = answers.get(name, {}).get(verb)
            if got not in ("yes", "no"):
                unproven.append(f"{verb} deployments/scale {label}: no clear answer ({got})")
            elif verb != "get" and got == "yes":
                refused.append(f"the authorizer allows {verb} on deployments/scale {label}")
            elif verb == "get" and got == "no":
                unproven.append(f"get deployments/scale {label} answered no: the control failed")
    for verb, res in DIRECT:
        got = answers.get(f"direct:{res}", {}).get(verb)
        if got not in ("yes", "no"):
            unproven.append(f"{verb} {res}: no clear answer ({got})")
        elif got == "yes":
            refused.append(f"the authorizer allows {verb} on {res}")
    for key in ["cluster"] + [f"ns:{n}" for n in collected.get("namespaces") or []]:
        for verb in ("update", "patch"):
            got = answers.get(key, {}).get(verb)
            where = "cluster-wide" if key == "cluster" else f"in namespace {key[3:]}"
            if got not in ("yes", "no"):
                unproven.append(f"{verb} deployments/scale {where}: no clear answer ({got})")
            elif got == "yes":
                refused.append(f"the authorizer allows {verb} on deployments/scale {where}")
    for kind in collected.get("list_errors") or []:
        unproven.append(f"the {kind} could not be listed: the binding audit is incomplete")
    user = f"system:serviceaccount:{sa_ns}:{sa}"
    groups = set(groups_of(sa_ns))

    def names_identity(subject, binding_ns):
        kind, name = subject.get("kind"), subject.get("name")
        if kind == "ServiceAccount":
            return name == sa and (subject.get("namespace") or binding_ns) == sa_ns
        return (kind == "User" and name == user) or (kind == "Group" and name in groups)

    def reaches(rule):
        g, res = set(rule.get("apiGroups") or []), set(rule.get("resources") or [])
        apps = bool(g & {"apps", "*"}) and bool(res & {"deployments/scale", "*/scale", "deployments", "*"})
        core = bool(g & {"", "*"}) and bool(res & {"pods", "*"})
        return apps or core
    objs = collected.get("rbac") or []
    roles = {}
    for o in objs:
        k = _role_kind(o)
        if k in ("ClusterRole", "Role"):
            roles[(k, o["metadata"].get("namespace") or "", o["metadata"]["name"])] = o
    bindings, grants = [], []
    for b in objs:
        if _role_kind(b) not in ("ClusterRoleBinding", "RoleBinding"):
            continue
        ns = b["metadata"].get("namespace") or ""
        subjects = [s for s in b.get("subjects") or [] if names_identity(s, ns)]
        if not subjects:
            continue
        ref = b.get("roleRef") or {}
        role = roles.get((ref.get("kind"), ns if ref.get("kind") == "Role" else "", ref.get("name")))
        bid = f'{b["kind"]}/{ns + "/" if ns else ""}{b["metadata"]["name"]}'
        rid = f'{ref.get("kind")}/{ns + "/" if ref.get("kind") == "Role" and ns else ""}{ref.get("name")}'
        bindings.append({"binding": bid, "role": rid, "role_found": role is not None,
                         "subjects": sorted(f'{s.get("kind")}:{s.get("name")}' for s in subjects)})
        if role is None:
            unproven.append(f"{bid} refers to {rid}, which was not found: its rules are unknown")
        for rule in (role or {}).get("rules") or []:
            if reaches(rule) and WRITES & set(rule.get("verbs") or []):
                gr = {"binding": bid, "role": rid, "where": ns or "(every namespace)", "verbs": rule.get("verbs"),
                      "apiGroups": rule.get("apiGroups"), "resources": rule.get("resources"),
                      "resourceNames": rule.get("resourceNames") or []}
                grants.append(gr)
                refused.append(f'{bid} -> {rid} authorizes {gr["verbs"]} on {gr["resources"]} in {gr["where"]}'
                               + (f', resourceNames {gr["resourceNames"]}' if gr["resourceNames"] else ""))
    v = "refused" if refused else "unproven" if unproven else "pass"
    return {"identity": user, "groups": sorted(groups), "targets": list(targets), "answers": answers,
            "bindings_of_the_identity": sorted(bindings, key=lambda x: x["binding"]), "write_grants": grants,
            "verdict": v, "refused": refused, "unproven": unproven}


def main(argv=None, kubectl_run=subprocess.run) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--record", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    ck = sub.add_parser("check")
    ck.add_argument("--targets", required=True, help="the planned target Deployment names, comma separated")
    ck.add_argument("--out", required=True)
    ck.add_argument("--sa", default="")
    ck.add_argument("--sa-namespace", default="")
    a = ap.parse_args(argv)
    import execlib as X
    import prepare
    record = X.load_record(a.record, ("prepare",))
    params = prepare.load_params(record, prepare.stage_of(prepare._section(record)))
    sa_ns = a.sa_namespace or params["control_namespace"]
    sa = a.sa or f"{params['release']}-predictive-autoscaler-operator"
    k = record.doc["kubeconfig"]

    def kubectl(args):
        p = kubectl_run(["kubectl", "--kubeconfig", k["path"], "--context", k["context"], "--request-timeout=30s",
                         *args], capture_output=True, text=True, timeout=120)
        return p.returncode, p.stdout or ""
    targets = [t for t in a.targets.split(",") if t]
    v = verdict(collect(kubectl, sa_ns, sa, params["namespace"], targets), sa_ns, sa, targets)
    X.write_new(a.out, json.dumps(v, indent=1, sort_keys=True) + "\n")
    X.receipt(record, "permissions", "check", {"out": a.out, "verdict": v["verdict"], "targets": targets,
                                               "refused": v["refused"][:10], "unproven": v["unproven"][:10]})
    print(json.dumps({"verdict": v["verdict"], "refused": len(v["refused"]), "unproven": len(v["unproven"])}))
    return {"pass": 0, "refused": 1}.get(v["verdict"], 3)


if __name__ == "__main__":
    sys.exit(main())
