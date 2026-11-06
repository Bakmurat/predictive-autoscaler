"""Replay datasets: content-hashed manifests over request-rate series with explicit availability (receipt) times.

A dataset is a manifest (forecast-dataset/v1, JSON) next to one CSV file `event_time,value_rps,available_at_ms`:
  - event_time: integer epoch seconds on the manifest's grid (an admitted v2 cadence), strictly increasing; a missing
    sample is an absent row, never a filled value;
  - value_rps: finite, non-negative requests/second;
  - available_at_ms: when the sample became available to the service (epoch ms), never before its event time. With
    availability mode 'settlement' every row must equal (event_time + settlement_seconds) * 1000 (the declared
    assumption when an archive lacks receipts); mode 'per_sample' admits any order of receipts.
The manifest pins the data file's bytes (data_sha256), the source, license and attribution, extraction command and
version, series selection, cadence, timezone (UTC only), units and exclusion windows. Its own digest
(manifest_sha256, over the canonical JSON) identifies the dataset in every replay record. Loading verifies all of it
and reads nothing but the two local files: no network, no cluster.

Synthetic generators authored for this project come first (deterministic, seeded with the standard library's
Mersenne Twister); licensed public traces get adapters later, each with its own license record.
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import random
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from forecasting.contracts import ALLOWED_INTERVALS, UNIT, canonical_json, sha256_hex

MANIFEST_SCHEMA = "forecast-dataset/v1"
CSV_HEADER = ("event_time", "value_rps", "available_at_ms")
AVAILABILITY_MODES = ("per_sample", "settlement")
SOURCE_KINDS = ("synthetic", "archive", "public")
PROJECT_LICENSE = {"name": "Apache-2.0", "url": "https://www.apache.org/licenses/LICENSE-2.0"}
MAX_DATA_BYTES = 256 << 20

_MANIFEST_KEYS = {"schema", "name", "version", "description", "source", "license", "attribution", "units",
                  "interval_seconds", "timezone", "series", "availability", "exclusions", "extraction", "data_file",
                  "data_sha256", "rows"}


class DatasetInvalid(Exception):
    pass


@dataclass(frozen=True)
class Observation:
    event_time: int
    value_rps: float
    available_at_ms: int


@dataclass(frozen=True)
class Dataset:
    manifest: Mapping
    manifest_sha256: str
    observations: Tuple[Observation, ...]

    @property
    def interval_seconds(self) -> int:
        return self.manifest["interval_seconds"]

    def excluded(self, t: int) -> Optional[str]:
        for ex in self.manifest["exclusions"]:
            if ex["start"] <= t < ex["end"]:
                return ex["reason"]
        return None


def _int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _encode_csv(observations: Sequence[Observation]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(CSV_HEADER)
    for o in observations:
        w.writerow((o.event_time, repr(float(o.value_rps)), o.available_at_ms))
    return buf.getvalue().encode("ascii")


def _check_observations(obs: Sequence[Observation], interval: int, availability: Mapping) -> None:
    last = None
    for o in obs:
        if not (_int(o.event_time) and _int(o.available_at_ms)):
            raise DatasetInvalid(f"row {o!r}: times must be integers")
        if o.event_time % interval:
            raise DatasetInvalid(f"event time {o.event_time} is off the {interval} s grid")
        if last is not None and o.event_time <= last:
            raise DatasetInvalid(f"event time {o.event_time} is duplicate or unordered")
        last = o.event_time
        if not (isinstance(o.value_rps, float) and math.isfinite(o.value_rps) and o.value_rps >= 0):
            raise DatasetInvalid(f"value at {o.event_time} is not a finite, non-negative rate")
        if o.available_at_ms < o.event_time * 1000:
            raise DatasetInvalid(f"sample {o.event_time} is available before it happened")
        if availability["mode"] == "settlement" and \
                o.available_at_ms != (o.event_time + availability["settlement_seconds"]) * 1000:
            raise DatasetInvalid(f"sample {o.event_time} does not follow the declared settlement delay")


def _check_manifest(m) -> None:
    if not isinstance(m, dict) or set(m) != _MANIFEST_KEYS:
        got = set(m) if isinstance(m, dict) else set()
        raise DatasetInvalid(f"manifest keys differ: missing {sorted(_MANIFEST_KEYS - got)}, "
                             f"extra {sorted(got - _MANIFEST_KEYS)}")
    if m["schema"] != MANIFEST_SCHEMA or m["units"] != UNIT or m["timezone"] != "UTC":
        raise DatasetInvalid("unsupported schema, units or timezone (UTC requests/second only)")
    if m["interval_seconds"] not in ALLOWED_INTERVALS:
        raise DatasetInvalid(f"interval {m['interval_seconds']!r} is not an admitted cadence")
    for k in ("name", "version", "description", "attribution", "series", "data_file"):
        if not isinstance(m[k], str) or not m[k]:
            raise DatasetInvalid(f"manifest {k} must be a non-empty string")
    df = m["data_file"]
    if os.path.basename(df) != df or df in (".", "..") or not df.endswith(".csv"):
        raise DatasetInvalid("data_file must be a plain .csv file name next to the manifest")
    src, lic, ext, av = m["source"], m["license"], m["extraction"], m["availability"]
    if not isinstance(src, dict) or src.get("kind") not in SOURCE_KINDS:
        raise DatasetInvalid(f"source.kind must be one of {SOURCE_KINDS}")
    if not isinstance(lic, dict) or not isinstance(lic.get("name"), str) or not lic["name"]:
        raise DatasetInvalid("every dataset names its license")
    if not isinstance(ext, dict) or not all(isinstance(ext.get(k), str) and ext[k] for k in ("command", "version")):
        raise DatasetInvalid("extraction needs a command and a version")
    if not isinstance(av, dict) or av.get("mode") not in AVAILABILITY_MODES or not _int(av.get("settlement_seconds")) \
            or av["settlement_seconds"] < 0 or set(av) != {"mode", "settlement_seconds"}:
        raise DatasetInvalid("availability must declare a mode and a settlement delay")
    if not isinstance(m["exclusions"], list):
        raise DatasetInvalid("exclusions must be a list")
    for ex in m["exclusions"]:
        if not (isinstance(ex, dict) and set(ex) == {"start", "end", "reason"} and _int(ex["start"]) and
                _int(ex["end"]) and ex["start"] < ex["end"] and isinstance(ex["reason"], str) and ex["reason"]):
            raise DatasetInvalid(f"malformed exclusion {ex!r}")
    if not (isinstance(m["data_sha256"], str) and len(m["data_sha256"]) == 64) or not _int(m["rows"]):
        raise DatasetInvalid("data_sha256 and rows are required")


def write_dataset(directory: str, *, name: str, version: str, description: str, source: Mapping,
                  interval_seconds: int, series: str, observations: Sequence[Observation],
                  availability: Mapping, extraction: Mapping, exclusions: Iterable[Mapping] = (),
                  license: Mapping = PROJECT_LICENSE,
                  attribution: str = "Synthetic series authored for the predictive-autoscaler project") -> str:
    """Write <name>.csv and <name>.manifest.json into directory; return the manifest path."""
    obs = list(observations)
    data = _encode_csv(obs)
    manifest = {"schema": MANIFEST_SCHEMA, "name": name, "version": version, "description": description,
                "source": dict(source), "license": dict(license), "attribution": attribution, "units": UNIT,
                "interval_seconds": interval_seconds, "timezone": "UTC", "series": series,
                "availability": dict(availability), "exclusions": [dict(e) for e in exclusions],
                "extraction": dict(extraction), "data_file": f"{name}.csv", "data_sha256": sha256_hex(data),
                "rows": len(obs)}
    _check_manifest(manifest)
    _check_observations(obs, interval_seconds, manifest["availability"])
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, manifest["data_file"]), "wb") as fh:
        fh.write(data)
    path = os.path.join(directory, f"{name}.manifest.json")
    with open(path, "w", encoding="ascii") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")
    return path


def load_dataset(manifest_path: str) -> Dataset:
    """Read and verify a dataset: manifest schema, data bytes against data_sha256, every row."""
    with open(manifest_path, "rb") as fh:
        try:
            manifest = json.loads(fh.read())
        except ValueError as e:
            raise DatasetInvalid(f"manifest is not JSON: {e}") from e
    _check_manifest(manifest)
    data_path = os.path.join(os.path.dirname(os.path.abspath(manifest_path)), manifest["data_file"])
    if os.path.getsize(data_path) > MAX_DATA_BYTES:
        raise DatasetInvalid("data file is larger than the replay limit")
    with open(data_path, "rb") as fh:
        data = fh.read()
    if sha256_hex(data) != manifest["data_sha256"]:
        raise DatasetInvalid("data file does not match the manifest's data_sha256")
    try:
        rows = list(csv.reader(io.StringIO(data.decode("ascii"))))
    except (UnicodeDecodeError, csv.Error) as e:
        raise DatasetInvalid(f"data file is not ASCII CSV: {e}") from e
    if not rows or tuple(rows[0]) != CSV_HEADER:
        raise DatasetInvalid(f"data header must be {','.join(CSV_HEADER)}")
    obs = []
    for r in rows[1:]:
        if len(r) != 3:
            raise DatasetInvalid(f"malformed row {r!r}")
        try:
            obs.append(Observation(int(r[0]), float(r[1]), int(r[2])))
        except ValueError as e:
            raise DatasetInvalid(f"malformed row {r!r}") from e
    if len(obs) != manifest["rows"]:
        raise DatasetInvalid("row count differs from the manifest")
    _check_observations(obs, manifest["interval_seconds"], manifest["availability"])
    return Dataset(manifest=manifest, manifest_sha256=sha256_hex(canonical_json(manifest)), observations=tuple(obs))


# --- synthetic series -------------------------------------------------------------------------------------------

def synthetic_values(kind: str, *, start: int, days: float, interval: int, seed: int = 0, base: float = 10.0,
                     amplitude: float = 5.0, noise: float = 0.5, step_at: Optional[int] = None,
                     step_factor: float = 2.0) -> List[Tuple[int, float]]:
    """[(event_time, rps)] on the grid: 'daily' (sinusoid + noise), 'constant', 'zero', 'step' (daily with a level
    change at step_at). Deterministic for a given seed."""
    if kind not in ("daily", "constant", "zero", "step") or start % interval:
        raise ValueError("unknown kind or an off-grid start")
    rng = random.Random(seed)
    out = []
    for i in range(int(days * 86400) // interval):
        t = start + i * interval
        if kind == "zero":
            v = 0.0
        elif kind == "constant":
            v = base
        else:
            v = base + amplitude * math.sin(2 * math.pi * (t % 86400) / 86400) + rng.gauss(0.0, noise)
            if kind == "step" and step_at is not None and t >= step_at:
                v *= step_factor
        out.append((t, round(max(0.0, v), 6)))
    return out


def with_receipts(values: Iterable[Tuple[int, float]], *, settlement_s: int,
                  delays_ms: Optional[Mapping[int, int]] = None, missing: Iterable[int] = ()) -> List[Observation]:
    """Observations available settlement_s after their event time, plus per-sample extra delays and dropped points."""
    gone, delays = set(missing), dict(delays_ms or {})
    return [Observation(t, float(v), (t + settlement_s) * 1000 + delays.get(t, 0)) for t, v in values if t not in gone]


def synthetic_dataset(directory: str, name: str, *, kind: str = "daily", start: int, days: float, interval: int,
                      seed: int = 0, settlement_s: int = 120, delays_ms: Optional[Dict[int, int]] = None,
                      missing: Iterable[int] = (), **shape) -> str:
    values = synthetic_values(kind, start=start, days=days, interval=interval, seed=seed, **shape)
    missing = list(missing)
    obs = with_receipts(values, settlement_s=settlement_s, delays_ms=delays_ms, missing=missing)
    mode = "per_sample" if delays_ms else "settlement"
    params = {"kind": kind, "start": start, "days": days, "interval": interval, "seed": seed, "missing": missing,
              "delays_ms": {str(k): v for k, v in sorted((delays_ms or {}).items())}, **shape}
    return write_dataset(directory, name=name, version="1", description=f"synthetic {kind} series",
                         source={"kind": "synthetic", "generator": "evaluation.datasets.synthetic_values",
                                 "parameters": params},
                         interval_seconds=interval, series=f"synthetic/{kind}", observations=obs,
                         availability={"mode": mode, "settlement_seconds": settlement_s},
                         extraction={"command": "evaluation.datasets.synthetic_dataset", "version": "1"})
