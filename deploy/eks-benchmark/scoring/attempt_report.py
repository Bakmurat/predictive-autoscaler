#!/usr/bin/env python3
"""Describe raw forecast RPC/cache evidence, never scientific origin availability.

Only the immutable byte buffer returned by forecast_log.load_verified is parsed.
All-prefix joins precede application/window filtering. Integrity here describes
observable ledger consistency, not capture completeness or forecast performance.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import tempfile

from forecast_log import load_verified

SCHEMA = "forecast-attempt-v1"
EVENTS = {"forecast_ledger_session", "forecast_attempt_started", "forecast_attempt_completed"}
IDENTITY = ("autoscaler_namespace", "autoscaler_name", "autoscaler_uid", "application", "namespace",
            "request_namespace", "metric_type", "horizon_minutes")
PAIR_FIELDS = ("operator_run_id", "lookup_id", "attempt_id", *IDENTITY,
               "request_body_sha256", "prior_issuance_id")
OUTCOMES = {"decoded_response", "http_refusal", "http_error", "transport_error", "decode_error", "timeout"}
RESOLUTIONS = {"fresh_response", "cache_hit", "stale_after_error", "unavailable", "local_error"}


def timestamp_ns(value):
    """Parse exact UTC nanoseconds; datetime alone would truncate RFC3339Nano."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("UTC timezone required")
        value = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z", str(value))
    if not match:
        raise ValueError("Expected UTC RFC3339 timestamp with Z suffix")
    parsed = datetime.fromisoformat(match[1] + "+00:00")
    seconds = int((parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds())
    return seconds * 1_000_000_000 + int((match[2] or "").ljust(9, "0"))


def strict_rows(buffer):
    if not isinstance(buffer, bytes):
        raise TypeError("Ledger reader requires immutable verified bytes")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON member " + key)
            result[key] = value
        return result
    def constant(value):
        raise ValueError("non-JSON numeric token " + value)
    def float_value(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("nonfinite JSON number")
        return result
    for line, text in enumerate(buffer.decode("utf-8").splitlines(), 1):
        if not text.strip():
            continue
        try:
            record = json.loads(text, object_pairs_hook=pairs, parse_constant=constant, parse_float=float_value)
            if not isinstance(record, dict):
                raise ValueError("record is not an object")
        except (ValueError, TypeError) as exc:
            raise ValueError(f"line {line}: {exc}") from exc
        yield line, record


def finite_number(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def one_of(value, options):
    return isinstance(value, str) and value in options


def valid_run(value):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{32}", value) is not None


def valid_lookup(value, run):
    return isinstance(value, str) and re.fullmatch(re.escape(run) + r":lookup:[1-9]\d*", value) is not None


def request_identity(record):
    return (all(isinstance(record.get(k), str) for k in IDENTITY if k != "horizon_minutes") and
            all(record.get(k) for k in ("autoscaler_namespace", "autoscaler_name", "application", "namespace", "metric_type")) and
            type(record.get("horizon_minutes")) is int and record["horizon_minutes"] > 0)


def completion_consistent(record):
    """Require the producer's HTTP/decode branches to agree, after type checks."""
    outcome, status = record["outcome"], record.get("http_status")
    error, stage = record["error_class"], record["error_stage"]
    served = record["served_predictions_status"]
    if outcome == "decoded_response":
        return (status == 200 and error == stage == "none" and
                served in {"array", "null", "absent"} and
                all(value in {"finite", "null"} for value in record["served_step_status"]))
    if outcome in {"http_refusal", "http_error"}:
        appropriate_status = (status is not None and
            (400 <= status < 500 if outcome == "http_refusal" else status != 200 and not 400 <= status < 500))
        return (appropriate_status and error in {"http_status", "timeout"} and
                stage == "response_body" and served == "not_evaluated")
    if outcome == "transport_error":
        return status is None and error == "transport" and stage == "request" and served == "not_evaluated"
    if outcome == "decode_error":
        return status == 200 and error == "decode" and stage == "decode" and served != "not_evaluated"
    if outcome == "timeout":
        return error == "timeout" and (
            status is None and stage == "request" and served == "not_evaluated" or
            status == 200 and stage == "decode" and served != "not_evaluated")
    return False


def summarize(buffer, app, namespace, start, end):
    """Retain duplicates and unresolved links; never infer unobserved API failures."""
    begin, finish = timestamp_ns(start), timestamp_ns(end)
    if begin >= finish:
        raise ValueError("start must precede end")
    issues, rows = [], list(strict_rows(buffer))
    sessions, sequences = defaultdict(list), defaultdict(list)
    starts, completions, issuances, decisions = (defaultdict(list) for _ in range(4))
    selected_legacy, selected_disabled, selected_decisions = [], [], []
    known_event_count = 0

    def issue(code, lines, severity="incomplete", **details):
        issues.append(dict(code=code, severity=severity, lines=sorted(set(lines)), **details))

    def selected(record, field="at"):
        if record.get("application") != app or record.get("namespace") != namespace:
            return False
        try:
            return begin <= timestamp_ns(record[field]) < finish
        except (KeyError, ValueError, TypeError):
            return False

    def check_identity(record, line):
        run = record.get("operator_run_id")
        if not valid_run(run) or not valid_lookup(record.get("lookup_id"), run or "") or not request_identity(record):
            issue("invalid_identity", [line], "invalid")
            return False
        return True

    for line, record in rows:
        event = record.get("event")
        if event is not None and not isinstance(event, str):
            issue("invalid_event_type", [line], "invalid")
            continue
        if event in EVENTS or (isinstance(event, str) and event.startswith(("forecast_attempt_", "forecast_ledger_"))) or "schema" in record:
            if event not in EVENTS or record.get("schema") != SCHEMA:
                issue("unknown_schema", [line], "invalid", schema=record.get("schema"), event=event)
                continue
            known_event_count += 1
            run, seq = record.get("operator_run_id"), record.get("event_seq")
            try:
                stamp = timestamp_ns(record.get("at"))
            except ValueError:
                issue("invalid_timestamp", [line], "invalid")
                continue
            if not valid_run(run) or type(seq) is not int or seq < 1 or "issued_at" in record:
                issue("invalid_event_header", [line], "invalid")
                continue
            sequences[run].append((seq, line, stamp))
            if event == "forecast_ledger_session":
                sessions[run].append((line, record))
                if seq != 1:
                    issue("session_sequence_not_one", [line], "invalid", operator_run_id=run)
                continue
            if not check_identity(record, line):
                continue
            if (record.get("attempt_id") != record["lookup_id"] + ":attempt" or
                    not isinstance(record.get("request_body_sha256"), str) or
                    not re.fullmatch("[0-9a-f]{64}", record["request_body_sha256"])):
                issue("invalid_attempt_identity", [line], "invalid")
                continue
            if event == "forecast_attempt_started":
                starts[record["attempt_id"]].append((line, record))
            else:
                try:
                    timestamp_ns(record["started_at"])
                    n, status, step_status = record["served_step_count"], record["served_predictions_status"], record["served_step_status"]
                    valid = (finite_number(record.get("elapsed_seconds")) and record["elapsed_seconds"] >= 0 and
                        (record.get("http_status") is None or type(record["http_status"]) is int and 100 <= record["http_status"] <= 599) and
                        record.get("outcome") in OUTCOMES and record.get("error_class") in {"none", "http_status", "transport", "decode", "timeout"} and
                        record.get("error_stage") in {"none", "request", "response_body", "decode"} and
                        status in {"array", "null", "absent", "malformed", "not_evaluated"} and type(n) is int and n >= 0 and
                        isinstance(step_status, list) and len(step_status) == n and
                        all(s in {"finite", "null", "malformed", "non_finite"} for s in step_status) and
                        (status == "array" or n == 0))
                except (KeyError, ValueError, TypeError):
                    valid = False
                if not valid:
                    issue("invalid_completion", [line], "invalid", attempt_id=record["attempt_id"])
                    continue
                if not completion_consistent(record):
                    issue("invalid_completion_semantics", [line], "invalid", attempt_id=record["attempt_id"], record=record)
                    continue
                completions[record["attempt_id"]].append((line, record))
        elif event == "decision":
            lookup = record.get("forecast_lookup")
            if lookup is None:
                if selected(record):
                    selected_legacy.append(dict(line=line, kind="decision", forecasting=record.get("forecasting")))
                continue
            if not isinstance(lookup, dict) or lookup.get("schema") != SCHEMA:
                issue("unknown_schema", [line], "invalid", schema=lookup.get("schema") if isinstance(lookup, dict) else None)
                continue
            instrumentation = lookup.get("instrumentation_status")
            if one_of(instrumentation, {"disabled", "initialization_failed"}):
                if selected(record):
                    selected_disabled.append(dict(line=line, instrumentation_status=instrumentation))
                continue
            if instrumentation != "enabled" or not check_identity(lookup, line):
                issue("invalid_lookup", [line], "invalid")
                continue
            if (not one_of(lookup.get("resolution"), RESOLUTIONS) or not one_of(lookup.get("cache_action"), {"replace", "keep", "delete", "none"}) or
                    not one_of(lookup.get("returned_link_status"), {"known", "none", "unavailable"}) or
                    not isinstance(record.get("forecast_status"), str) or
                    not (lookup.get("cache_age_seconds") is None or finite_number(lookup["cache_age_seconds"])) or
                    any(lookup.get(k) is not None and not isinstance(lookup[k], str) for k in ("fresh_attempt_id", "prior_issuance_id", "returned_issuance_id"))):
                issue("invalid_lookup", [line], "invalid")
                continue
            if (record.get("application"), record.get("namespace")) != (lookup["application"], lookup["namespace"]):
                issue("decision_identity_mismatch", [line], "invalid")
            try:
                timestamp_ns(record["at"])
            except (KeyError, ValueError):
                issue("invalid_timestamp", [line], "invalid")
            decisions[lookup["lookup_id"]].append((line, record))
            if selected(record):
                selected_decisions.append((line, record))
        elif not event and "issued_at" in record:
            if not any(record.get(k) for k in ("operator_run_id", "lookup_id", "attempt_id", "issuance_id")):
                if selected(record, "issued_at"):
                    selected_legacy.append(dict(line=line, kind="issuance"))
                continue
            run, lookup, aid, iid = (record.get(k) for k in ("operator_run_id", "lookup_id", "attempt_id", "issuance_id"))
            if not valid_run(run) or not valid_lookup(lookup, run or "") or aid != str(lookup) + ":attempt" or iid != str(aid) + ":issuance":
                issue("invalid_issuance_identity", [line], "invalid")
                continue
            issuances[iid].append((line, record))

    for run, entries in sequences.items():
        counts = Counter(seq for seq, _, _ in entries)
        duplicates = [seq for seq, count in counts.items() if count > 1]
        if duplicates:
            issue("duplicate_sequence", [line for seq, line, _ in entries if seq in duplicates], "invalid", sequences=duplicates, operator_run_id=run)
        unique = sorted(counts)
        gaps = [[a + 1, b - 1] for a, b in zip([0] + unique, unique) if b > a + 1]
        if gaps:
            issue("sequence_gap", [entries[0][1], entries[-1][1]], ranges_inclusive=gaps, operator_run_id=run)
        if any(a[0] > b[0] for a, b in zip(entries, entries[1:])):
            issue("sequence_out_of_order", [e[1] for e in entries], "invalid", operator_run_id=run)
        if any(a[2] > b[2] for a, b in zip(entries, entries[1:])):
            issue("clock_reversal", [e[1] for e in entries], "diagnostic", operator_run_id=run)
    for run in set(sequences) | {r["forecast_lookup"]["operator_run_id"] for items in decisions.values() for _, r in items}:
        if not sessions[run]:
            issue("missing_session", [line for _, line, _ in sequences.get(run, [])], operator_run_id=run)
        elif len(sessions[run]) > 1:
            issue("duplicate_session", [line for line, _ in sessions[run]], "invalid", operator_run_id=run)

    for mapping, code in ((starts, "duplicate_start"), (completions, "duplicate_completion"), (issuances, "duplicate_issuance"), (decisions, "duplicate_decision")):
        for key, entries in mapping.items():
            if len(entries) > 1:
                issue(code, [line for line, _ in entries], "invalid", identity=key,
                      identical=all(r == entries[0][1] for _, r in entries))

    def resolve_issuance(iid, owner, lines):
        if iid is None:
            return
        if not isinstance(iid, str) or not re.fullmatch(re.escape(owner["operator_run_id"]) + r":lookup:[1-9]\d*:attempt:issuance", iid):
            issue("invalid_issuance_link", lines, "invalid")
            return
        entries = issuances.get(iid, [])
        if not entries:
            issue("missing_issuance", lines, issuance_id=iid)
        for line, record in entries:
            # Legacy issuance.namespace is the exact wire namespace; the new
            # ledger namespace is the effective target fallback when wire is empty.
            if (any(record.get(k) != owner.get(k) for k in ("operator_run_id", "application")) or
                    record.get("namespace") != owner.get("request_namespace")):
                issue("issuance_identity_mismatch", lines + [line], "invalid", issuance_id=iid)

    attempts = []
    for aid in dict.fromkeys([*starts, *completions]):
        s, c = starts.get(aid, []), completions.get(aid, [])
        if not s:
            issue("missing_start", [line for line, _ in c], attempt_id=aid)
            continue
        sl, source = s[0]
        lookup_id = source["lookup_id"]
        ds = decisions.get(lookup_id, [])
        resolve_issuance(source.get("prior_issuance_id"), source, [sl])
        if not c:
            issue("missing_completion", [sl], attempt_id=aid)
        if not ds:
            issue("missing_decision", [sl] + [line for line, _ in c], lookup_id=lookup_id)
        for cl, terminal in c:
            if any(source.get(k) != terminal.get(k) for k in PAIR_FIELDS) or source["at"] != terminal.get("started_at"):
                issue("identity_mismatch", [sl, cl], "invalid", attempt_id=aid)
            if cl < sl:
                issue("completion_before_start", [sl, cl], "invalid", attempt_id=aid)
            if terminal.get("outcome") == "decoded_response" and terminal.get("served_step_count", 0) > 0:
                resolve_issuance(aid + ":issuance", source, [sl, cl])
            for dl, decision in ds:
                lookup = decision["forecast_lookup"]
                if lookup.get("fresh_attempt_id") != aid or any(source.get(k) != lookup.get(k) for k in IDENTITY):
                    issue("decision_attempt_mismatch", [sl, cl, dl], "invalid", attempt_id=aid)
                if dl < cl:
                    issue("decision_before_completion", [cl, dl], "invalid", attempt_id=aid)
        terminal = c[0][1] if len(c) == 1 else {}
        decision = ds[0][1] if len(ds) == 1 else {}
        lookup = decision.get("forecast_lookup", {})
        if any(selected(r) for _, r in s):
            lines = [sl] + [line for line, _ in c] + [line for line, _ in ds]
            attempts.append(dict(attempt_id=aid, lookup_id=lookup_id, operator_run_id=source["operator_run_id"],
                started_at=source["at"], completed_at=terminal.get("at"),
                outcome=terminal.get("outcome"), error_class=terminal.get("error_class"), error_stage=terminal.get("error_stage"),
                http_status=terminal.get("http_status"), elapsed_seconds=terminal.get("elapsed_seconds"),
                served_predictions_status=terminal.get("served_predictions_status"), served_step_count=terminal.get("served_step_count"),
                served_step_status=terminal.get("served_step_status"),
                issuance_id=aid + ":issuance" if aid + ":issuance" in issuances else None,
                prior_issuance_id=source.get("prior_issuance_id"), returned_issuance_id=lookup.get("returned_issuance_id"),
                resolution=lookup.get("resolution"), decision_forecast_status=decision.get("forecast_status"),
                source_lines=lines, selected_start_lines=[line for line, r in s if selected(r)],
                start_record=source, start_records=[r for _, r in s], completion_records=[r for _, r in c]))

    for lookup_id, entries in decisions.items():
        for line, decision in entries:
            lookup = decision["forecast_lookup"]
            aid, returned = lookup.get("fresh_attempt_id"), lookup.get("returned_issuance_id")
            if aid is not None:
                if aid != lookup_id + ":attempt":
                    issue("invalid_attempt_link", [line], "invalid", attempt_id=aid)
                if aid not in starts:
                    issue("missing_start", [line], attempt_id=aid)
                if aid not in completions:
                    issue("missing_completion", [line], attempt_id=aid)
            if lookup["resolution"] in {"fresh_response", "stale_after_error"} and aid is None:
                issue("missing_attempt_link", [line], lookup_id=lookup_id)
            if lookup["resolution"] == "cache_hit" and aid is not None:
                issue("cache_hit_has_attempt", [line], "invalid", lookup_id=lookup_id)
            terminal = completions.get(aid, [])
            if len(terminal) == 1:
                cl, completion = terminal[0]
                decoded = completion["outcome"] == "decoded_response"
                if (lookup["resolution"] == "fresh_response" and not decoded or
                        lookup["resolution"] in {"stale_after_error", "unavailable"} and decoded):
                    issue("outcome_resolution_conflict", [cl, line], "invalid", attempt_id=aid)
                if lookup["resolution"] == "fresh_response":
                    expected = aid + ":issuance" if completion["served_step_count"] > 0 else None
                    if returned != expected:
                        issue("fresh_issuance_link_conflict", [cl, line], "invalid", attempt_id=aid)
            expected_action = {"fresh_response": "replace", "cache_hit": "keep", "stale_after_error": "keep"}.get(lookup["resolution"])
            if expected_action is not None and lookup["cache_action"] != expected_action:
                issue("cache_action_conflict", [line], "invalid", lookup_id=lookup_id)
            if lookup["resolution"] == "stale_after_error" and returned != lookup.get("prior_issuance_id"):
                issue("stale_issuance_link_conflict", [line], "invalid", lookup_id=lookup_id)
            if (lookup.get("returned_link_status") == "known") != bool(returned):
                issue("returned_link_conflict", [line], "invalid", lookup_id=lookup_id)
            resolve_issuance(returned, lookup, [line])
            resolve_issuance(lookup.get("prior_issuance_id"), lookup, [line])
    for iid, entries in issuances.items():
        for line, issuance in entries:
            aid = issuance["attempt_id"]
            if aid not in starts:
                issue("missing_start", [line], attempt_id=aid)
            if aid not in completions:
                issue("missing_completion", [line], attempt_id=aid)
            for sl, source in starts.get(aid, []):
                if (any(source.get(k) != issuance.get(k) for k in ("operator_run_id", "lookup_id", "application")) or
                        source.get("request_namespace") != issuance.get("namespace")):
                    issue("issuance_identity_mismatch", [sl, line], "invalid", issuance_id=iid)
                if line < sl:
                    issue("issuance_before_start", [sl, line], "invalid", issuance_id=iid)
            for cl, terminal in completions.get(aid, []):
                if terminal.get("outcome") != "decoded_response" or terminal.get("served_step_count", 0) == 0:
                    issue("unexpected_issuance", [cl, line], "invalid", issuance_id=iid)

    for attempt in attempts:
        relevant = [i for i in issues if (set(i["lines"]) & set(attempt["source_lines"]) or i.get("attempt_id") == attempt["attempt_id"]) and i["severity"] != "diagnostic"]
        attempt["status"] = ("invalid" if any(i["severity"] == "invalid" for i in relevant) else
                             "pending_or_unresolved" if relevant else "resolved")
        attempt["issues"] = [i["code"] for i in relevant]
    window_instrumented = bool(attempts or selected_decisions or selected_disabled)
    state = "mixed" if window_instrumented and selected_legacy else "observed" if window_instrumented else "not_instrumented"
    integrity = ("invalid" if any(i["severity"] == "invalid" for i in issues) else
                 "incomplete" if any(i["severity"] == "incomplete" for i in issues) else
                 "consistent" if window_instrumented else "not_instrumented")
    outcomes = Counter(a["outcome"] for a in attempts if a["outcome"] is not None)
    stale = Counter()
    for _, d in selected_decisions:
        lookup = d["forecast_lookup"]
        if lookup["resolution"] == "stale_after_error":
            terminal = completions.get(lookup.get("fresh_attempt_id"), [])
            stale[terminal[0][1].get("error_class", "unknown") if len(terminal) == 1 else "unknown"] += 1
    return dict(schema="forecast-attempt-report-v1", app=app, namespace=namespace, window=[str(start), str(end)],
        formal_availability=dict(status="not_evaluated", coverage=None,
            reason="Fresh RPC cadence depends on failures and cache state; no scientific opportunity denominator is declared."),
        instrumentation=dict(selected_window=state, ledger_events_in_prefix=known_event_count,
            selected_legacy_unlinked_records=selected_legacy, selected_disabled_records=selected_disabled),
        evidence_integrity=dict(status=integrity, scope="Entire verified prefix, before app/time filtering; consistency is not completeness."),
        counts=dict(starts=sum(len(a["selected_start_lines"]) for a in attempts), attempt_identities=len(attempts),
            completion_records=sum(len(a["completion_records"]) for a in attempts),
            completed_unique=sum(a["outcome"] is not None for a in attempts),
            outcomes=dict(outcomes), unresolved=sum(a["status"] != "resolved" for a in attempts),
            observed_failed_completions=sum(n for k, n in outcomes.items() if k != "decoded_response") if window_instrumented else None,
            cache_resolutions=dict(Counter(r["forecast_lookup"]["resolution"] for _, r in selected_decisions)),
            decision_forecast_statuses=dict(Counter(r.get("forecast_status", "unknown") for _, r in selected_decisions)),
            stale_after_error_classes=dict(stale)),
        attempts=attempts, selected_decisions=[dict(line=line, record=r) for line, r in selected_decisions], issues=issues,
        limitations=["Attempt selection uses start in [start,end); completions and links may lie outside it in this prefix.",
            "Decision summaries independently use decision time in [start,end); their population can differ.",
            "event_seq covers session/start/completion only; missing cache-hit-only decisions cannot be detected by sequence gaps.",
            "Absent final tails, whole processes, throttled or early-return reconciles are not measured by this ledger.",
            "Historical missing attempts and API component outcomes hidden behind refusals remain unknown.",
            "Raw served-step status is not model accuracy, formal coverage, or proof of API input validity."])


def write_result(value, destination):
    text = json.dumps(value, indent=2, allow_nan=False) + "\n"
    if destination == "-":
        print(text, end="")
        return
    fd, temporary = tempfile.mkstemp(prefix=".attempt-report-", dir=Path(destination).parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text)
        os.link(temporary, destination)
    finally:
        os.unlink(temporary)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--forecast-log", required=True)
    parser.add_argument("--forecast-receipt", required=True)
    parser.add_argument("--allow-fixture-receipt", action="store_true", help="TEST ONLY")
    parser.add_argument("--app", default="nginx-test")
    parser.add_argument("--namespace", default="demo")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out", default="-")
    args = parser.parse_args(argv)
    try:
        timestamp_ns(args.start)
        timestamp_ns(args.end)
        # load_verified requires datetime; floor only for receipt probe comparison.
        # Compare exact nanoseconds again so sub-microsecond window tails cannot escape.
        boundary = datetime.fromisoformat(args.end.replace("Z", "+00:00"))
        data, provenance = load_verified(args.forecast_log, args.forecast_receipt, boundary, args.allow_fixture_receipt)
        if timestamp_ns(args.end) > timestamp_ns(provenance["receipt"]["remote_probe_at"]):
            raise ValueError("window ends after remote probe")
        result = summarize(data, args.app, args.namespace, args.start, args.end)
        result["forecast_log_provenance"] = provenance
        write_result(result, args.out)
    except (ValueError, TypeError, OSError, UnicodeError) as exc:
        raise SystemExit("FAIL: attempt report: " + str(exc)) from exc
    return 0


if __name__ == "__main__":
    main()
