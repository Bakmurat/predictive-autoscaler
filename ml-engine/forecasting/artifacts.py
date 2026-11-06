"""Statistical artifacts: a versioned JSON manifest plus numeric arrays, bound to one signal, loaded without pickle.

An artifact is one deterministic ZIP (stored, fixed timestamps) with exactly three members: manifest.json and the .npy
arrays hw_season and ar_coef. Its identity is the sha256 of those exact bytes. decode() checks that digest against the
one the caller expects BEFORE parsing anything, then reads the arrays with np.load(..., allow_pickle=False) and checks
each array's own digest, dtype and shape against the manifest.

The manifest binds the fit to its signal and recipe (Binding: autoscaler UID, target UID, query hash, signal contract,
cadence, seasonality, recipe) and records the fit cutoff, the digest of the exact input values, the kernel settings and
the Holt-Winters/AR parameters. An artifact is refused (ArtifactRefused, with a bounded reason) when it is:
  - digest:       not the expected bytes;
  - malformed:    any member, field, dtype or shape off the schema;
  - foreign:      bound to another autoscaler, Deployment, query, contract or recipe;
  - incompatible: fitted for another cadence, seasonality or kernel settings;
  - stale:        its cutoff is after the input end (from the future) or older than the maximum fit age.
ArtifactStore keeps content-addressed files and one pointer per signal, each written to a temporary file, fsynced and
renamed (atomic promotion); a promotion never replaces a newer cutoff with an older one.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import threading
import zipfile
from dataclasses import asdict, dataclass
from typing import Dict, Optional, Tuple

import numpy as np

from forecasting.contracts import canonical_json, sha256_hex
from forecasting.statistical import RECIPE_E1, E1Fit, HWState, KernelSettings, KernelUnavailable, Series, fit_e1

ARTIFACT_SCHEMA = "statistical-artifact/v1"
MEMBERS = ("manifest.json", "hw_season.npy", "ar_coef.npy")
MAX_ARTIFACT_BYTES = 1 << 20
MAX_FIT_AGE_SECONDS = 12 * 3600
_ZIP_TIME = (1980, 1, 1, 0, 0, 0)


class ArtifactRefused(Exception):
    REASONS = ("digest", "malformed", "foreign", "incompatible", "stale")

    def __init__(self, reason: str, message: str):
        if reason not in self.REASONS:
            raise ValueError(f"unknown refusal reason {reason!r}")
        super().__init__(f"{reason}: {message}")
        self.reason = reason


@dataclass(frozen=True)
class Binding:
    """What an artifact belongs to. key() names its files; nothing caller-controlled reaches a path."""
    autoscaler_uid: str
    target_uid: str
    query_sha256: str
    signal_contract: str
    interval_seconds: int
    seasonality: str
    recipe: str

    def key(self) -> str:
        return sha256_hex(canonical_json({"schema": "artifact-binding/v1", **asdict(self)}))

    @classmethod
    def for_signal(cls, signal, settings, recipe: str = RECIPE_E1) -> "Binding":
        """From an api.identity.ResolvedSignal and forecasting.contracts.ForecastSettings."""
        return cls(signal.autoscaler_uid, signal.target_uid, signal.sha256, signal.contract,
                   settings.sample_interval_seconds, settings.seasonality, recipe)


def _npy(a: np.ndarray) -> bytes:
    buf = io.BytesIO()
    np.lib.format.write_array(buf, np.ascontiguousarray(a, dtype="<f8"), allow_pickle=False)
    return buf.getvalue()


def encode_e1(fit: E1Fit, binding: Binding) -> bytes:
    """The artifact bytes of an E1-product fit (deterministic: the same fit gives the same bytes)."""
    if binding.recipe != RECIPE_E1 or binding.interval_seconds != fit.interval_seconds:
        raise ValueError("binding does not describe this fit")
    arrays = {"hw_season": np.array(fit.hw.season, dtype="<f8"), "ar_coef": np.array(fit.ar_coef, dtype="<f8")}
    blobs = {f"{k}.npy": _npy(v) for k, v in arrays.items()}
    hw = {k: v for k, v in asdict(fit.hw).items() if k != "season"}
    manifest = {"schema": ARTIFACT_SCHEMA, "recipe": RECIPE_E1, "binding": asdict(binding),
                "fit_cutoff": fit.cutoff, "input_lo": fit.input_lo, "input_sha256": fit.input_sha256,
                "kernel_settings": fit.settings, "hw": hw, "ar_rows": fit.ar_rows,
                "arrays": {k: {"dtype": "<f8", "shape": list(v.shape), "sha256": sha256_hex(blobs[f"{k}.npy"])}
                           for k, v in arrays.items()}}
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_STORED) as z:
        for name, data in (("manifest.json", canonical_json(manifest)), *sorted(blobs.items())):
            info = zipfile.ZipInfo(name, date_time=_ZIP_TIME)
            info.external_attr = 0o644 << 16
            z.writestr(info, data)
    return out.getvalue()


def decode(data: bytes, expected_sha256: str, binding: Binding, ks: KernelSettings, *, input_end: int,
           max_age_seconds: int = MAX_FIT_AGE_SECONDS) -> E1Fit:
    """The fit in `data`, after every check in the module docstring. Raises ArtifactRefused."""
    if not isinstance(data, (bytes, bytearray)) or len(data) > MAX_ARTIFACT_BYTES or sha256_hex(bytes(data)) != \
            expected_sha256:
        raise ArtifactRefused("digest", "the artifact bytes are not the expected ones")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            if sorted(z.namelist()) != sorted(MEMBERS):
                raise ArtifactRefused("malformed", f"members {z.namelist()}")
            raw = {n: z.read(n) for n in MEMBERS}
        manifest = json.loads(raw["manifest.json"])
        arrays = {}
        for name in ("hw_season", "ar_coef"):
            spec = manifest["arrays"][name]
            blob = raw[f"{name}.npy"]
            if sha256_hex(blob) != spec["sha256"]:
                raise ArtifactRefused("malformed", f"{name} does not match its digest")
            a = np.load(io.BytesIO(blob), allow_pickle=False)
            if a.dtype != np.dtype("<f8") or list(a.shape) != spec["shape"] or not np.all(np.isfinite(a)):
                raise ArtifactRefused("malformed", f"{name} has the wrong dtype, shape or values")
            arrays[name] = a
        if manifest["schema"] != ARTIFACT_SCHEMA or manifest["recipe"] != RECIPE_E1:
            raise ArtifactRefused("malformed", "unknown schema or recipe")
        hw = HWState(**manifest["hw"], season=tuple(float(v) for v in arrays["hw_season"]))
        fit = E1Fit(cutoff=manifest["fit_cutoff"], interval_seconds=manifest["binding"]["interval_seconds"], hw=hw,
                    ar_coef=tuple(float(v) for v in arrays["ar_coef"]), ar_rows=manifest["ar_rows"],
                    input_lo=manifest["input_lo"], input_sha256=manifest["input_sha256"],
                    settings=manifest["kernel_settings"])
        bound = Binding(**manifest["binding"])
    except ArtifactRefused:
        raise
    except (zipfile.BadZipFile, ValueError, KeyError, TypeError) as e:
        raise ArtifactRefused("malformed", f"{type(e).__name__}: {e}") from e
    if bound != binding:
        same_signal = (bound.autoscaler_uid, bound.target_uid, bound.query_sha256, bound.signal_contract,
                       bound.recipe) == (binding.autoscaler_uid, binding.target_uid, binding.query_sha256,
                                         binding.signal_contract, binding.recipe)
        raise ArtifactRefused("incompatible" if same_signal else "foreign", "the artifact is bound elsewhere")
    if fit.settings != ks.canonical() or len(hw.season) != ks.m or len(fit.ar_coef) != ks.ar_order + 1:
        raise ArtifactRefused("incompatible", "fitted with other kernel settings")
    if fit.cutoff > input_end:
        raise ArtifactRefused("stale", "the fit cutoff is after the input end")
    if input_end - fit.cutoff > max_age_seconds:
        raise ArtifactRefused("stale", f"the fit is older than {max_age_seconds} s")
    return fit


def fit_e1_artifact(payload: Dict) -> bytes:
    """The worker's fit entry point (runs in the fit process): payload -> artifact bytes. Raises KernelUnavailable."""
    values = np.frombuffer(payload["values"], dtype="<f8")
    series = Series(payload["start"], payload["interval"], values.copy())
    ks = KernelSettings(**payload["kernel"])
    binding = Binding(**payload["binding"])
    return encode_e1(fit_e1(series, payload["cutoff"], ks), binding)


def fit_payload(series: Series, cutoff: int, ks: KernelSettings, binding: Binding) -> Dict:
    """A picklable, bounded payload for fit_e1_artifact (values up to the cutoff only: nothing later is sent)."""
    hi = series.index(cutoff)
    return {"start": series.start, "interval": series.interval, "cutoff": cutoff, "kernel": ks.canonical(),
            "binding": asdict(binding),
            "values": np.ascontiguousarray(series.values[:hi + 1], dtype="<f8").tobytes()}


class ArtifactStore:
    """Content-addressed artifact files plus one pointer per binding key, promoted atomically."""

    def __init__(self, directory: str):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, name: str) -> str:
        return os.path.join(self.directory, name)

    def _write_atomic(self, name: str, data: bytes) -> None:
        tmp = self._path(f".{name}.tmp")
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._path(name))
        dfd = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)

    def current(self, binding: Binding) -> Optional[Tuple[str, int, bytes]]:
        """(sha256, cutoff, bytes) of the promoted artifact, or None. The bytes are checked against the pointer."""
        try:
            with open(self._path(f"{binding.key()}.json"), "rb") as fh:
                ptr = json.loads(fh.read())
            with open(self._path(f"{ptr['sha256']}.zip"), "rb") as fh:
                data = fh.read(MAX_ARTIFACT_BYTES + 1)
        except FileNotFoundError:
            return None
        except (OSError, ValueError, KeyError) as e:
            raise ArtifactRefused("malformed", f"unreadable pointer or artifact: {e}") from e
        if sha256_hex(data) != ptr["sha256"]:
            raise ArtifactRefused("digest", "the promoted artifact does not match its pointer")
        return ptr["sha256"], ptr["fit_cutoff"], data

    def promote(self, binding: Binding, data: bytes, ks: KernelSettings) -> Tuple[str, bool]:
        """Validate and promote; (sha256, promoted). A cutoff not newer than the current one is not promoted."""
        sha = sha256_hex(data)
        fit = decode(data, sha, binding, ks, input_end=2 ** 62, max_age_seconds=2 ** 62)   # identity/schema checks
        with self._lock:
            cur = self.current(binding)
            if cur is not None and fit.cutoff <= cur[1]:
                return cur[0], False
            self._write_atomic(f"{sha}.zip", data)
            self._write_atomic(f"{binding.key()}.json", canonical_json({"sha256": sha, "fit_cutoff": fit.cutoff}))
            return sha, True

    def load_snapshot(self, binding: Binding, ks: KernelSettings, *, input_end: int) -> Optional[Tuple[str, E1Fit]]:
        """(sha256, fit) decoded from ONE read of the promoted bytes, so a concurrent promotion can never pair one
        artifact's fit with another's digest. None when nothing is promoted; ArtifactRefused when stale or invalid."""
        cur = self.current(binding)
        if cur is None:
            return None
        sha, _, data = cur
        return sha, decode(data, sha, binding, ks, input_end=input_end)

    def load(self, binding: Binding, ks: KernelSettings, *, input_end: int) -> Optional[E1Fit]:
        """The promoted fit, usable at input_end (raises ArtifactRefused when it is stale or invalid), or None."""
        cur = self.current(binding)
        return None if cur is None else decode(cur[2], cur[0], binding, ks, input_end=input_end)


__all__ = ["ArtifactRefused", "ArtifactStore", "Binding", "KernelUnavailable", "decode", "encode_e1",
           "fit_e1_artifact", "fit_payload"]
