"""C2.3: statistical artifacts (no pickle, exact-byte digests, stale and foreign refusals), atomic promotion and the
bounded fit worker (one fit per signal, deadline, memory limit, kill before release)."""

import dataclasses
import io
import json
import os
import sys
import zipfile

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting import artifacts as A  # noqa: E402
from forecasting import statistical as S  # noqa: E402
from forecasting import worker as W  # noqa: E402
from forecasting.contracts import sha256_hex  # noqa: E402
from forecasting.statistical import KernelSettings, Series  # noqa: E402

DAY = 86400
T0 = 1_792_022_400
DT = 600
KS = KernelSettings(DT)
BINDING = A.Binding("pa-uid", "dep-uid", "a" * 64, "requests-per-second/v1", DT, "daily", S.RECIPE_E1)


@pytest.fixture(scope="module")
def series():
    m = DAY // DT
    t = np.arange(15 * m)
    rng = np.random.default_rng(11)
    return Series(T0, DT, np.maximum(80 + 30 * np.sin(2 * np.pi * t / m) + rng.normal(0, 2, len(t)), 0))


@pytest.fixture(scope="module")
def fit(series):
    return S.fit_e1(series, series.end, KS)


@pytest.fixture(scope="module")
def blob(fit):
    return A.encode_e1(fit, BINDING)


# --- artifacts ----------------------------------------------------------------------------------------------------

def test_an_artifact_round_trips_exactly_and_deterministically(fit, blob):
    assert A.encode_e1(fit, BINDING) == blob                       # same fit, same bytes, same digest
    back = A.decode(blob, sha256_hex(blob), BINDING, KS, input_end=fit.cutoff)
    assert back == fit
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        assert sorted(z.namelist()) == sorted(A.MEMBERS)
        assert all(i.date_time == (1980, 1, 1, 0, 0, 0) for i in z.infolist())
        manifest = json.loads(z.read("manifest.json"))
    assert manifest["schema"] == "statistical-artifact/v1" and manifest["binding"]["autoscaler_uid"] == "pa-uid"
    assert manifest["fit_cutoff"] == fit.cutoff and manifest["input_sha256"] == fit.input_sha256


def rebuild(blob, replace=None, drop=None, add=None):
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        members = {n: z.read(n) for n in z.namelist()}
    members.update(replace or {})
    for n in drop or ():
        members.pop(n)
    members.update(add or {})
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        for n, d in members.items():
            z.writestr(n, d)
    return out.getvalue()


def manifest_of(blob):
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        return json.loads(z.read("manifest.json"))


def pickled_array():
    buf = io.BytesIO()
    np.save(buf, np.array([{"a": 1}], dtype=object), allow_pickle=True)
    return buf.getvalue()


@pytest.mark.parametrize("make,reason", [
    (lambda b: rebuild(b, replace={"ar_coef.npy": pickled_array()}), "malformed"),          # pickle never loads
    (lambda b: rebuild(b, drop=["hw_season.npy"]), "malformed"),
    (lambda b: rebuild(b, add={"extra.npy": b"x"}), "malformed"),
    (lambda b: rebuild(b, replace={"manifest.json": b"{"}), "malformed"),
    (lambda b: b"not a zip", "malformed"),
])
def test_malformed_artifacts_are_refused(blob, make, reason):
    bad = make(blob)
    with pytest.raises(A.ArtifactRefused) as e:
        A.decode(bad, sha256_hex(bad), BINDING, KS, input_end=T0 + 15 * DAY)
    assert e.value.reason == reason


def test_an_array_that_does_not_match_its_manifest_is_refused(blob):
    m = manifest_of(blob)
    m["arrays"]["ar_coef"]["shape"] = [5]
    bad = rebuild(blob, replace={"manifest.json": json.dumps(m).encode()})
    with pytest.raises(A.ArtifactRefused):
        A.decode(bad, sha256_hex(bad), BINDING, KS, input_end=T0 + 15 * DAY)
    other = np.array([1.0, 2.0, 3.0, 4.0])
    buf = io.BytesIO()
    np.save(buf, other)
    bad = rebuild(blob, replace={"ar_coef.npy": buf.getvalue()})                     # digest in manifest differs
    with pytest.raises(A.ArtifactRefused):
        A.decode(bad, sha256_hex(bad), BINDING, KS, input_end=T0 + 15 * DAY)


def test_the_exact_bytes_are_checked_first(blob, fit):
    with pytest.raises(A.ArtifactRefused) as e:
        A.decode(blob + b"\0", sha256_hex(blob), BINDING, KS, input_end=fit.cutoff)
    assert e.value.reason == "digest"


@pytest.mark.parametrize("change,reason", [
    ({"autoscaler_uid": "other-pa"}, "foreign"), ({"target_uid": "new-dep"}, "foreign"),
    ({"query_sha256": "b" * 64}, "foreign"), ({"signal_contract": "x"}, "foreign"),
    ({"recipe": S.RECIPE_NAIVE}, "foreign"),
    ({"interval_seconds": 300}, "incompatible"), ({"seasonality": "dailyWeekly"}, "incompatible"),
])
def test_foreign_and_incompatible_artifacts_are_refused(blob, fit, change, reason):
    with pytest.raises(A.ArtifactRefused) as e:
        A.decode(blob, sha256_hex(blob), dataclasses.replace(BINDING, **change), KS, input_end=fit.cutoff)
    assert e.value.reason == reason


def test_other_kernel_settings_are_incompatible(blob, fit):
    with pytest.raises(A.ArtifactRefused) as e:
        A.decode(blob, sha256_hex(blob), BINDING, KernelSettings(DT, profile_min_support=4), input_end=fit.cutoff)
    assert e.value.reason == "incompatible"


def test_stale_artifacts_are_refused(blob, fit):
    A.decode(blob, sha256_hex(blob), BINDING, KS, input_end=fit.cutoff + 12 * 3600)          # 12 h: still usable
    for end in (fit.cutoff + 12 * 3600 + DT, fit.cutoff - DT):                                 # too old / from the future
        with pytest.raises(A.ArtifactRefused) as e:
            A.decode(blob, sha256_hex(blob), BINDING, KS, input_end=end)
        assert e.value.reason == "stale"


def test_promotion_is_atomic_and_never_goes_back(tmp_path, series, blob, fit):
    store = A.ArtifactStore(str(tmp_path))
    assert store.current(BINDING) is None and store.load(BINDING, KS, input_end=fit.cutoff) is None
    sha, promoted = store.promote(BINDING, blob, KS)
    assert promoted and store.current(BINDING) == (sha, fit.cutoff, blob)
    older = A.encode_e1(S.fit_e1(series, series.end - 6 * 3600, KS), BINDING)
    assert store.promote(BINDING, older, KS) == (sha, False)                              # an older cutoff loses
    assert store.load(BINDING, KS, input_end=fit.cutoff) == fit
    assert sorted(os.listdir(tmp_path)) == sorted([f"{sha}.zip", f"{BINDING.key()}.json"])  # no temporary files
    with pytest.raises(A.ArtifactRefused):                                                 # foreign bytes
        store.promote(dataclasses.replace(BINDING, autoscaler_uid="other"), blob, KS)
    (tmp_path / f"{sha}.zip").write_bytes(blob[:-1] + b"\1")                               # damaged on disk
    with pytest.raises(A.ArtifactRefused) as e:
        store.load(BINDING, KS, input_end=fit.cutoff)
    assert e.value.reason == "digest"


# --- the fit worker -----------------------------------------------------------------------------------------------

HELPERS = {S.RECIPE_E1: "tests.c2_fit_helpers:quick_fit"}


def payload(series, cutoff=None, bound=BINDING, **extra):
    return {**A.fit_payload(series, cutoff or series.end, KS, bound), **extra}


def until_running(w, n=1):
    import time
    deadline = time.monotonic() + 30
    while len(w._running) < n and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(w._running) >= n


def binding(n):
    return dataclasses.replace(BINDING, autoscaler_uid=f"pa-{n}")


def worker(tmp_path, ref="tests.c2_fit_helpers:quick_fit", **kw):
    return W.FitWorker(A.ArtifactStore(str(tmp_path)), fits={S.RECIPE_E1: ref}, **kw)


def test_a_real_fit_runs_in_its_own_process_and_is_promoted(tmp_path, series, blob, fit):
    w = W.FitWorker(A.ArtifactStore(str(tmp_path)))
    try:
        r = w.wait(w.submit(BINDING, S.RECIPE_E1, payload(series)), timeout=120)
    finally:
        w.close()
    assert r.status == "promoted" and r.artifact_sha256 == sha256_hex(blob) and r.cpu_seconds > 0
    assert A.ArtifactStore(str(tmp_path)).load(BINDING, KS, input_end=fit.cutoff) == fit


def test_a_timed_out_fit_is_killed_before_its_slot_is_released(tmp_path, series, blob):
    store = A.ArtifactStore(str(tmp_path))
    store.promote(BINDING, blob, KS)
    w = W.FitWorker(store, fits={S.RECIPE_E1: "tests.c2_fit_helpers:sleep_fit"}, deadline_s=1.0)
    try:
        slow = w.submit(BINDING, S.RECIPE_E1, payload(series, sleep=60))
        r = w.wait(slow, timeout=60)
        assert r.status == "failed" and r.reason == "timeout" and 1.0 <= r.finished - r.started < 10
        assert w._running == {} and w.failures["timeout"] == 1
    finally:
        w.close()
    assert store.current(BINDING)[0] == sha256_hex(blob)                     # the earlier fit stays promoted


def test_fits_run_one_at_a_time_and_one_per_signal(tmp_path, series, blob):
    w = worker(tmp_path, ref="tests.c2_fit_helpers:sleep_fit", deadline_s=1.5, max_pending=2)
    try:
        a = w.submit(binding(1), S.RECIPE_E1, payload(series, bound=binding(1), sleep=60))
        until_running(w)
        assert w.submit(binding(1), S.RECIPE_E1, payload(series, bound=binding(1))) is a   # coalesced: one per signal
        b = w.submit(binding(2), S.RECIPE_E1, payload(series, bound=binding(2), sleep=60))
        c = w.submit(binding(3), S.RECIPE_E1, payload(series, bound=binding(3), sleep=60))
        with pytest.raises(W.QueueFull):
            w.submit(binding(4), S.RECIPE_E1, payload(series, bound=binding(4)))
        ra, rb = w.wait(a, 60), w.wait(b, 60)
        assert rb.started >= ra.finished                       # the cancelled fit was gone before the next began
        assert w.wait(c, 60).started >= rb.finished
    finally:
        w.close()


@pytest.mark.parametrize("ref,reason", [
    ("tests.c2_fit_helpers:hog_fit", "resource"), ("tests.c2_fit_helpers:crash_fit", "resource"),
    ("tests.c2_fit_helpers:history_fit", "history"), ("tests.c2_fit_helpers:bad_reason_fit", "numeric"),
])
def test_fit_failures_carry_a_bounded_reason(tmp_path, series, ref, reason):
    w = worker(tmp_path, ref=ref, memory_bytes=384 << 20)
    try:
        r = w.wait(w.submit(BINDING, S.RECIPE_E1, payload(series)), timeout=60)
    finally:
        w.close()
    assert (r.status, r.reason) == ("failed", reason) and w.failures[reason] == 1
    assert A.ArtifactStore(str(tmp_path)).current(BINDING) is None


def test_bad_artifact_bytes_are_a_storage_failure(tmp_path, series, blob):
    w = worker(tmp_path)
    try:
        r = w.wait(w.submit(BINDING, S.RECIPE_E1, payload(series, artifact=b"junk")), timeout=60)
        ok = w.wait(w.submit(BINDING, S.RECIPE_E1, payload(series, artifact=blob)), timeout=60)
        again = w.wait(w.submit(BINDING, S.RECIPE_E1, payload(series, artifact=blob)), timeout=60)
    finally:
        w.close()
    assert (r.status, r.reason) == ("failed", "storage")
    assert ok.status == "promoted" and again.status == "unchanged"


def test_submissions_are_bounded_and_allowlisted(tmp_path, series):
    w = worker(tmp_path)
    try:
        with pytest.raises(W.FitRefused):
            w.submit(BINDING, "lstm/v1", payload(series))
        with pytest.raises(W.FitRefused):
            w.submit(dataclasses.replace(BINDING, recipe=S.RECIPE_NAIVE), S.RECIPE_E1, payload(series))
        with pytest.raises(W.FitRefused):
            w.submit(BINDING, S.RECIPE_E1, {**payload(series), "values": b"\0" * (W.MAX_PAYLOAD_BYTES + 8)})
        with pytest.raises(W.FitRefused):                                       # a payload for another signal
            w.submit(binding(9), S.RECIPE_E1, payload(series))
    finally:
        w.close()
    with pytest.raises(W.FitRefused):
        w.submit(BINDING, S.RECIPE_E1, payload(series))


def test_close_fails_queued_fits(tmp_path, series):
    w = worker(tmp_path, ref="tests.c2_fit_helpers:sleep_fit", deadline_s=30)
    running = w.submit(binding(1), S.RECIPE_E1, payload(series, bound=binding(1), sleep=60))
    until_running(w)
    queued = w.submit(binding(2), S.RECIPE_E1, payload(series, bound=binding(2), sleep=60))
    w.close()
    assert w.wait(queued, 5).reason == "resource"
    assert w.wait(running, 15).status == "failed"


def test_the_payload_carries_nothing_after_the_cutoff(series):
    p = A.fit_payload(series, series.end - 10 * DT, KS, BINDING)
    assert len(p["values"]) // 8 == series.index(series.end - 10 * DT) + 1
    assert W.FAILURE_REASONS == S.FAILURE_REASONS


# --- lifecycle: one deadline for result and exit; shutdown races (Codex review) -----------------------------------

def test_a_fit_that_sends_its_result_but_does_not_exit_is_killed_and_not_promoted(tmp_path, series, blob):
    w = worker(tmp_path, ref="tests.c2_fit_helpers:lingering_fit", deadline_s=1.0)
    try:
        r = w.wait(w.submit(BINDING, S.RECIPE_E1, payload(series, artifact=blob)), timeout=60)
    finally:
        w.close()
    assert (r.status, r.reason) == ("failed", "timeout") and r.artifact_sha256 is None
    assert 1.0 <= r.finished - r.started < 4.0 and w._running == {}
    assert A.ArtifactStore(str(tmp_path)).current(BINDING) is None


def paused_close(w, stage):
    """Install a hook that pauses the dispatcher at `stage` until released; returns (entered, release, threading, time)."""
    import threading
    import time
    entered, release = threading.Event(), threading.Event()

    def hook(at, t):
        if at == stage:
            entered.set()
            assert release.wait(30)
    w._hook = hook
    return entered, release, threading, time


@pytest.mark.parametrize("stage,message", [("dequeued", "before the fit started"),
                                           ("publish", "before publication")])
def test_close_resolves_an_admitted_fit_and_publishes_nothing(tmp_path, series, blob, stage, message):
    w = worker(tmp_path)
    entered, release, threading, time = paused_close(w, stage)
    t = w.submit(BINDING, S.RECIPE_E1, payload(series, artifact=blob))
    assert entered.wait(60)                             # dequeued (not yet started) / fitted (not yet published)
    closer = threading.Thread(target=w.close)
    closer.start()
    for _ in range(3000):
        if w._closed:
            break
        time.sleep(0.01)
    assert w._closed and closer.is_alive()              # close() waits for the admitted ticket
    release.set()
    closer.join(60)
    assert not closer.is_alive() and t.done.is_set()    # resolved by the time close() returned
    assert (t.result.status, t.result.reason) == ("failed", "resource") and message in t.result.message
    assert A.ArtifactStore(str(tmp_path)).current(BINDING) is None and w._running == {}
    with pytest.raises(W.FitRefused):
        w.submit(BINDING, S.RECIPE_E1, payload(series, artifact=blob))


def test_a_snapshot_decodes_the_bytes_its_digest_names(tmp_path, series, blob, fit):
    store = A.ArtifactStore(str(tmp_path))
    assert store.load_snapshot(BINDING, KS, input_end=fit.cutoff) is None
    older_fit = S.fit_e1(series, series.end - 6 * 3600, KS)
    older = A.encode_e1(older_fit, BINDING)
    sha_a, _ = store.promote(BINDING, older, KS)
    real = store.current

    def current_then_promote(binding):                 # a newer promotion lands right after the one read
        out = real(binding)
        A.ArtifactStore(str(tmp_path)).promote(BINDING, blob, KS)
        return out
    store.current = current_then_promote
    got_sha, got_fit = store.load_snapshot(BINDING, KS, input_end=fit.cutoff)
    assert (got_sha, got_fit) == (sha_a, older_fit)    # A's digest with A's fit, never A's digest with B's fit
    assert real(BINDING)[0] == sha256_hex(blob)        # B is promoted now, for the next read
