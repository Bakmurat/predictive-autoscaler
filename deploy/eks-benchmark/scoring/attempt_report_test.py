"""Descriptive attempt-ledger tests; every fixture is synthetic."""
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import attempt_report as report
import forecast_log
import score

RUN = "a" * 32
T0 = datetime(2030, 1, 1, tzinfo=timezone.utc)


def stamp(seconds):
    return (T0 + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def encode(rows):
    return ("\n".join(json.dumps(r, allow_nan=False) for r in rows) + "\n").encode()


def identity(app="a", ns="n"):
    return dict(autoscaler_namespace="n", autoscaler_name="pa-" + app,
                autoscaler_uid="uid-" + app, application=app, namespace=ns,
                request_namespace=ns, metric_type="requests", horizon_minutes=60)


def session(run=RUN, at=0):
    return dict(schema="forecast-attempt-v1", event="forecast_ledger_session",
                operator_run_id=run, event_seq=1, at=stamp(at))


def call(number=1, seq=2, at=1, app="a", ns="n", run=RUN, prior=None,
         outcome="decoded_response", count=6, resolution=None):
    lookup = f"{run}:lookup:{number}"
    aid, iid = lookup + ":attempt", lookup + ":attempt:issuance"
    start = dict(schema="forecast-attempt-v1", event="forecast_attempt_started",
                 operator_run_id=run, event_seq=seq, at=stamp(at), lookup_id=lookup,
                 attempt_id=aid, request_body_sha256="f" * 64, **identity(app, ns))
    if prior:
        start["prior_issuance_id"] = prior
    success = outcome == "decoded_response"
    completion = dict(start, event="forecast_attempt_completed", event_seq=seq + 1,
        at=stamp(at + 1), started_at=stamp(at), elapsed_seconds=1.,
        http_status=200 if success else 503, outcome=outcome,
        error_class="none" if success else "http_status", error_stage="none" if success else "response_body",
        served_predictions_status="array" if success else "not_evaluated",
        served_step_count=count if success else 0,
        served_step_status=["finite"] * count if success else [])
    lookup_data = dict(schema="forecast-attempt-v1", instrumentation_status="enabled",
        operator_run_id=run, lookup_id=lookup, **identity(app, ns),
        resolution=resolution or ("fresh_response" if success else "unavailable"),
        cache_action="replace" if success else "none", cache_age_seconds=None,
        fresh_attempt_id=aid, returned_link_status="known" if success and count else "none")
    issuance = None
    if success and count:
        lookup_data["returned_issuance_id"] = iid
        issuance = dict(application=app, namespace=ns, issued_at=stamp(at + 1),
            operator_run_id=run, lookup_id=lookup, attempt_id=aid, issuance_id=iid,
            horizon_minutes=60, step_minutes=10., confidence=.9, model_version="synthetic",
            training_cutoff=stamp(-3600), target_anchor="issued_at",
            forecasts=[dict(step=h, target_at=stamp(at + 1 + h * 600), rpm=100.) for h in range(1, count + 1)])
    decision = dict(event="decision", application=app, namespace=ns, at=stamp(at + 2),
                    forecasting=True, forecast_status="used" if success and count else "unavailable",
                    forecast_lookup=lookup_data)
    return [start, completion] + ([issuance] if issuance else []) + [decision]


def analyze(rows, app="a", start=0, end=600):
    return report.summarize(encode(rows), app, "n", stamp(start), stamp(end))


class AttemptReportTests(unittest.TestCase):
    def test_success_links_and_formal_availability_never_evaluated(self):
        result = analyze([session(), *call()])
        self.assertEqual(result["evidence_integrity"]["status"], "consistent")
        self.assertEqual(result["attempts"][0]["status"], "resolved")
        self.assertEqual(result["attempts"][0]["outcome"], "decoded_response")
        self.assertEqual(result["counts"]["starts"], 1)
        self.assertEqual(result["formal_availability"]["status"], "not_evaluated")
        self.assertIsNone(result["formal_availability"]["coverage"])

    def test_whole_prefix_links_before_app_window_filter(self):
        a, b = call(at=1), call(number=2, seq=3, at=3, app="other")
        a[1]["at"], a[-1]["at"] = stamp(650), stamp(652)
        a[1]["event_seq"], a[2]["issued_at"] = 5, stamp(650)
        result = analyze([session(), a[0], *b, *a[1:]], end=600)
        self.assertEqual(result["attempts"][0]["status"], "resolved")
        self.assertEqual(result["attempts"][0]["completed_at"], stamp(650))
        self.assertFalse(any(i["code"] == "sequence_gap" for i in result["issues"]))
        self.assertEqual(result["counts"]["starts"], 1)

    def test_cache_hit_and_failed_refresh_stale_use_remain_distinct(self):
        first = call()
        iid = first[2]["issuance_id"]
        hit = copy.deepcopy(first[-1])
        hit["at"] = stamp(30)
        hit["forecast_lookup"].update(lookup_id=RUN + ":lookup:2", resolution="cache_hit",
                                      cache_action="keep", cache_age_seconds=28.)
        hit["forecast_lookup"].pop("fresh_attempt_id")
        hit["forecast_lookup"]["prior_issuance_id"] = iid
        failure = call(3, 4, 310, prior=iid, outcome="http_error", resolution="stale_after_error")
        failure[-1]["forecast_status"] = "used"
        failure[-1]["forecast_lookup"].update(cache_action="keep", cache_age_seconds=309.,
            returned_issuance_id=iid, prior_issuance_id=iid, returned_link_status="known")
        result = analyze([session(), *first, hit, *failure])
        self.assertEqual(result["counts"]["starts"], 2)
        self.assertEqual(result["counts"]["cache_resolutions"]["cache_hit"], 1)
        self.assertEqual(result["counts"]["stale_after_error_classes"]["http_status"], 1)
        self.assertEqual(result["attempts"][1]["outcome"], "http_error")
        self.assertEqual(result["attempts"][1]["returned_issuance_id"], iid)

    def test_pending_start_is_not_failed_call(self):
        result = analyze([session(), call()[0]])
        self.assertEqual(result["attempts"][0]["status"], "pending_or_unresolved")
        self.assertEqual(result["counts"]["observed_failed_completions"], 0)
        self.assertEqual(result["evidence_integrity"]["status"], "incomplete")

    def test_missing_predecessors_and_expected_records(self):
        rows = call()
        for drop, code in ((0, "missing_start"), (2, "missing_issuance"), (3, "missing_decision")):
            with self.subTest(code=code):
                result = analyze([session(), *[r for i, r in enumerate(rows) if i != drop]])
                self.assertIn(code, [i["code"] for i in result["issues"]])
                self.assertNotEqual(result["evidence_integrity"]["status"], "consistent")

    def test_duplicate_completion_and_conflicting_identity_not_cleaned(self):
        rows = [session(), *call()]
        duplicate = analyze([*rows, copy.deepcopy(rows[2])])
        self.assertIn("duplicate_completion", [i["code"] for i in duplicate["issues"]])
        self.assertEqual(duplicate["evidence_integrity"]["status"], "invalid")
        rows[2]["application"] = "other"
        conflict = analyze(rows)
        self.assertIn("identity_mismatch", [i["code"] for i in conflict["issues"]])

    def test_sequence_gap_restart_unknown_schema(self):
        rows = [session(), *call()]
        rows[2]["event_seq"] = 4
        result = analyze(rows)
        self.assertIn("sequence_gap", [i["code"] for i in result["issues"]])
        other = "b" * 32
        clean = analyze([session(), *call(), session(other, 100), *call(at=101, run=other)])
        self.assertEqual(clean["evidence_integrity"]["status"], "consistent")
        rows = [session(), *call()]
        rows[1]["schema"] = "forecast-attempt-v99"
        self.assertIn("unknown_schema", [i["code"] for i in analyze(rows)["issues"]])

    def test_empty_decoded_response_does_not_require_issuance(self):
        result = analyze([session(), *call(count=0)])
        self.assertEqual(result["evidence_integrity"]["status"], "consistent")
        self.assertIsNone(result["attempts"][0]["issuance_id"])

    def test_issuance_can_precede_deferred_completion_append(self):
        rows = call()
        result = analyze([session(), rows[0], rows[2], rows[1], rows[3]])
        self.assertEqual(result["evidence_integrity"]["status"], "consistent")

    def test_empty_wire_namespace_preserves_effective_target_links(self):
        rows = call()
        rows[0]["request_namespace"] = ""
        rows[1]["request_namespace"] = ""
        rows[2]["namespace"] = ""
        rows[-1]["forecast_lookup"]["request_namespace"] = ""
        result = analyze([session(), *rows])
        self.assertEqual(result["evidence_integrity"]["status"], "consistent")
        self.assertEqual(result["counts"]["starts"], 1)

    def test_actual_go_synthetic_fixture_contract(self):
        path = Path(__file__).resolve().parents[3] / "k8s-operator/controllers/testdata/forecast_attempt_ledger_synthetic.jsonl"
        buffer = path.read_bytes()
        rows = [r for _, r in report.strict_rows(buffer)]
        first = next(r for r in rows if r.get("event") == "forecast_attempt_started")
        day = first["at"][:10]
        result = report.summarize(buffer, first["application"], first["namespace"], day+"T00:00:00Z", day+"T23:59:59Z")
        self.assertEqual(result["evidence_integrity"]["status"], "consistent")
        self.assertEqual(result["counts"]["starts"], 3)
        self.assertEqual(result["counts"]["observed_failed_completions"], 2)
        self.assertEqual(result["counts"]["cache_resolutions"],
                         dict(fresh_response=1, cache_hit=1, stale_after_error=1, unavailable=1))

    def test_malformed_schema_fields_remain_explicit_not_reader_crash(self):
        for field, value in (("served_step_count", "six"), ("outcome", []), ("http_status", True)):
            with self.subTest(field=field):
                rows = [session(), *call()]
                rows[2][field] = value
                result = analyze(rows)
                self.assertEqual(result["evidence_integrity"]["status"], "invalid")
        rows = [session(), *call()]
        rows[-1]["forecast_lookup"]["resolution"] = []
        self.assertEqual(analyze(rows)["evidence_integrity"]["status"], "invalid")
        for kind in ("prior_issuance_id", "forecast_status"):
            rows = [session(), *call()]
            rows[1 if kind == "prior_issuance_id" else -1][kind] = []
            self.assertEqual(analyze(rows)["evidence_integrity"]["status"], "invalid")

    def test_cache_resolution_cannot_rewrite_rpc_or_fresh_issuance(self):
        first, second = call(), call(2, 4, 310)
        second[-1]["forecast_lookup"]["returned_issuance_id"] = first[2]["issuance_id"]
        result = analyze([session(), *first, *second])
        self.assertIn("fresh_issuance_link_conflict", [i["code"] for i in result["issues"]])
        second[-1]["forecast_lookup"].update(resolution="stale_after_error", cache_action="keep")
        result = analyze([session(), *first, *second])
        self.assertIn("outcome_resolution_conflict", [i["code"] for i in result["issues"]])

    def test_raw_null_finite_and_nonfinite_statuses_are_distinct(self):
        rows = call(outcome="decode_error")
        rows[1].update(http_status=200, error_class="decode", error_stage="decode",
                       served_predictions_status="array", served_step_count=6)
        rows[1]["served_step_status"] = ["finite", "null", "non_finite", "malformed", "finite", "finite"]
        result = analyze([session(), *rows])
        self.assertEqual(result["attempts"][0]["served_step_status"], rows[1]["served_step_status"])
        self.assertEqual(result["attempts"][0]["outcome"], "decode_error")

    def test_timeout_during_refusal_body_keeps_http_outcome(self):
        rows = call(outcome="http_refusal")
        rows[1].update(http_status=422, error_class="timeout", error_stage="response_body")
        rows[-1]["forecast_lookup"]["cache_action"] = "delete"
        result = analyze([session(), *rows])
        self.assertEqual(result["attempts"][0]["outcome"], "http_refusal")
        self.assertEqual(result["attempts"][0]["error_class"], "timeout")

    def test_historical_unlinked_records_are_not_instrumented(self):
        old = call()[2]
        for key in ("operator_run_id", "lookup_id", "attempt_id", "issuance_id"):
            old.pop(key)
        result = analyze([old])
        self.assertEqual(result["instrumentation"]["selected_window"], "not_instrumented")
        self.assertEqual(result["evidence_integrity"]["status"], "not_instrumented")
        self.assertIsNone(result["counts"]["observed_failed_completions"])

    def test_strict_json_and_nanosecond_window_boundary(self):
        for raw in (b'{"event":', b'[]\n', b'{"event":"x","event":"y"}\n', b'{"x":NaN}\n'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                report.summarize(raw, "a", "n", stamp(0), stamp(10))
        rows = [session(), *call()]
        rows[1]["at"] = "2030-01-01T00:00:01.000000900Z"
        rows[2]["started_at"] = rows[1]["at"]
        result = report.summarize(encode(rows), "a", "n", stamp(0), "2030-01-01T00:00:01.000000500Z")
        self.assertEqual(result["counts"]["starts"], 0)

    def test_cli_uses_exact_verified_bytes_and_rejects_bad_receipt(self):
        data = encode([session(), *call()])
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            log, receipt, out = base/"log", base/"receipt", base/"report.json"
            log.write_bytes(data)
            meta = dict(schema_version=1, kind="fixture", bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                reader_fingerprint="f"*64, remote_probe_at=stamp(900), collected_at=stamp(901),
                source={k: "fixture" for k in forecast_log.SOURCE_FIELDS})
            receipt.write_text(json.dumps(meta))
            args = ["--forecast-log", str(log), "--forecast-receipt", str(receipt), "--allow-fixture-receipt",
                    "--app", "a", "--namespace", "n", "--start", stamp(0), "--end", stamp(600), "--out", str(out)]
            seen = []
            original = report.summarize
            def capture(buffer, *a, **kw):
                seen.append(buffer)
                log.write_text("changed after verified snapshot")
                return original(buffer, *a, **kw)
            with patch.object(report, "summarize", side_effect=capture):
                self.assertEqual(report.main(args), 0)
            self.assertEqual(seen, [data])
            self.assertTrue(json.loads(out.read_text())["forecast_log_provenance"]["fixture"])
            with self.assertRaises(SystemExit):
                report.main(args[:-1] + [str(base/"other.json")])

    def test_mixed_events_leave_existing_scoring_and_components_equal(self):
        rows = call()
        legacy = copy.deepcopy(rows[2])
        for key in ("operator_run_id", "lookup_id", "attempt_id", "issuance_id"):
            legacy.pop(key)
        class Prom:
            def instant(self, query, at):
                return 100.
        outputs = []
        for buffer in (encode([legacy]), encode([session(), *rows])):
            accepted, rejected = score.accept_records(score.load_forecasts(buffer, "a", "n", T0, T0+timedelta(minutes=10)))
            value, scored = score.score(accepted, Prom(), "a", "n", T0, T0+timedelta(minutes=10))
            outputs.append((value, scored, rejected))
        self.assertEqual(outputs[0], outputs[1])

    def test_mixed_events_leave_participation_equal(self):
        rows = call()
        old = copy.deepcopy(rows[-1])
        old.pop("forecast_lookup")
        old.update(desired_source="prediction", raw_predicted_replicas=3,
                   confidence_adjusted_replicas=3, predicted_replicas=3, safeguards=[])
        rows[-1].update(old)
        outputs = []
        for buffer in (encode([old]), encode([session(), *rows])):
            decisions = score.load_decisions(buffer, "a", "n", T0, T0+timedelta(minutes=10))
            outputs.append(score.participation(decisions))
        self.assertEqual(outputs[0], outputs[1])


def case(outcome='decoded_response', count=6, **changes):
    rows = [session(), *call(outcome=outcome, count=count)]
    rows[2].update(changes)
    return analyze(rows), rows[2]


class CompletionSemantics(unittest.TestCase):
    def test_impossible_completion_combinations_are_invalid(self):
        cases = [
            dict(http_status=422, error_class='http_status', error_stage='response_body'),
            dict(http_status=None),
            dict(outcome='http_refusal', http_status=200, error_class='none', error_stage='none'),
            dict(outcome='transport_error', http_status=200, error_class='transport', error_stage='request'),
            dict(outcome='http_error', http_status=404),
            dict(outcome='http_error', http_status=200),
            dict(outcome='decode_error', http_status=None, error_class='decode', error_stage='decode', served_predictions_status='malformed'),
            dict(outcome='timeout', http_status=None, error_class='transport', error_stage='request'),
            dict(outcome='transport_error', http_status=None, error_class='transport', error_stage='request', served_predictions_status='array', served_step_count=1, served_step_status=['finite']),
            dict(served_step_status=['finite', 'null', 'non_finite', 'malformed', 'finite', 'finite']),
            dict(count=0, served_predictions_status='not_evaluated'),
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                result, completion = case(**changes)
                self.assertEqual(result['evidence_integrity']['status'], 'invalid')
                defects = [i for i in result['issues'] if i['code'] == 'invalid_completion_semantics']
                self.assertEqual(len(defects), 1)
                self.assertEqual(defects[0]['record'], completion)
                self.assertEqual(result['counts']['completed_unique'], 0)
                self.assertIsNone(result['attempts'][0]['outcome'])
                self.assertEqual(result['formal_availability']['status'], 'not_evaluated')

    def test_valid_decoded_empty_absent_null_and_null_steps_remain_valid(self):
        cases = [
            {},
            dict(served_step_status=['null']*6),
            dict(count=0),
            dict(count=0, served_predictions_status='null'),
            dict(count=0, served_predictions_status='absent'),
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                result, _ = case(**changes)
                self.assertEqual(result['evidence_integrity']['status'], 'consistent')
                self.assertEqual(result['counts']['completed_unique'], 1)

    def test_real_timeout_branches_keep_original_http_outcome(self):
        cases = [
            dict(outcome='http_refusal', http_status=422, error_class='timeout', error_stage='response_body'),
            dict(outcome='http_error', http_status=503, error_class='timeout', error_stage='response_body'),
            dict(outcome='timeout', http_status=None, error_class='timeout', error_stage='request'),
            dict(outcome='timeout', http_status=200, error_class='timeout', error_stage='decode', served_predictions_status='malformed'),
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                result, _ = case(**changes)
                self.assertEqual(result['evidence_integrity']['status'], 'consistent')
                self.assertEqual(result['attempts'][0]['outcome'], changes['outcome'])

    def test_other_response_field_can_fail_decode_despite_valid_served_steps(self):
        for status, count, steps in [('array', 2, ['finite', 'null']), ('null', 0, []), ('absent', 0, []), ('malformed', 0, [])]:
            with self.subTest(status=status):
                result, _ = case(outcome='decode_error', http_status=200, error_class='decode', error_stage='decode',
                                 served_predictions_status=status, served_step_count=count, served_step_status=steps)
                self.assertEqual(result['evidence_integrity']['status'], 'consistent')


if __name__ == "__main__":
    unittest.main()
