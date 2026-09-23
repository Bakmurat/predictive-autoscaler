"""Bind a saved log to a collector's verified remote prefix (not authentication).

A receipt establishes transport integrity under an append-only source assumption.
It cannot prove that the operator wrote every expected record. The scorer still
checks coverage. In-place rewriting, truncation or rotation of FORECAST_LOG
invalidates this append-only precondition. Probe time is recorded just before
remote execution, so the issuance-window bound is conservative. Never manufacture a remote receipt from a local hash alone.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from datetime import datetime, timezone

KIND = 'pvc-forecast-log-prefix'
SOURCE_FIELDS = ('context', 'namespace', 'pvc', 'pvc_uid', 'pv', 'path',
                 'operator_pod', 'operator_pod_uid', 'node')


def utc(value):
    if not isinstance(value, str) or not value.endswith('Z'):
        raise ValueError('receipt time must be UTC with Z suffix')
    return datetime.fromisoformat(value[:-1] + '+00:00')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def validate(data, receipt, allow_fixture=False):
    """Validate exactly the byte buffer that the caller will parse."""
    if not isinstance(receipt, dict) or type(receipt.get('schema_version')) is not int or receipt['schema_version'] != 1:
        raise ValueError('unsupported forecast receipt schema')
    fixture = receipt.get('kind') == 'fixture'
    if receipt.get('kind') != KIND and not (fixture and allow_fixture):
        raise ValueError('a remote prefix receipt is required (fixtures need explicit opt-in)')
    if type(receipt.get('bytes')) is not int or receipt['bytes'] < 0:
        raise ValueError('invalid receipt byte count')
    for field in ('sha256', 'reader_fingerprint'):
        if not isinstance(receipt.get(field), str) or not re.fullmatch('[0-9a-f]{64}', receipt[field]):
            raise ValueError('invalid receipt ' + field)
    if len(data) != receipt['bytes'] or digest(data) != receipt['sha256']:
        raise ValueError('forecast bytes or SHA-256 differ from remote prefix receipt')
    probe, collected = utc(receipt.get('remote_probe_at')), utc(receipt.get('collected_at'))
    if collected < probe:
        raise ValueError('collection precedes remote probe')
    source = receipt.get('source')
    if not isinstance(source, dict) or any(not isinstance(source.get(k), str) or not source[k].strip() for k in SOURCE_FIELDS):
        raise ValueError('missing source identity in forecast receipt')
    return fixture


def load_verified(path, receipt_path, end, allow_fixture=False):
    """Read once, verify once, return immutable bytes for BOTH scorer loaders."""
    try:
        raw_receipt = Path(receipt_path).read_bytes()
        receipt = json.loads(raw_receipt)
        data = Path(path).read_bytes()
        fixture = validate(data, receipt, allow_fixture)
        if end > utc(receipt['remote_probe_at']):
            raise ValueError('issuance window ends after remote probe')
    except (OSError, ValueError, TypeError) as exc:
        raise SystemExit('FAIL: forecast receipt: ' + str(exc)) from exc
    return data, {'fixture': fixture, 'receipt_sha256': digest(raw_receipt),
                  'receipt': receipt, 'integrity_only': True}


def emit(path, receipt_path, size, sha, probe_at, source, reader_fingerprint):
    """Publish a receipt only after saved bytes match the actual remote probe."""
    receipt_path = Path(receipt_path)
    if receipt_path.exists():
        raise ValueError('receipt destination already exists')
    receipt = dict(schema_version=1, kind=KIND, bytes=size, sha256=sha,
                   remote_probe_at=probe_at,
                   collected_at=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                   source=source, reader_fingerprint=reader_fingerprint)
    validate(Path(path).read_bytes(), receipt)
    fd, tmp = tempfile.mkstemp(prefix='.receipt-', dir=receipt_path.parent)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(receipt, f, indent=2)
            f.write('\n')
        # Exclusive publication; never replace an earlier receipt in a race.
        os.link(tmp, receipt_path)
    finally:
        os.unlink(tmp)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--log', required=True); ap.add_argument('--receipt', required=True)
    ap.add_argument('--remote-bytes', type=int, required=True)
    ap.add_argument('--remote-sha256', required=True); ap.add_argument('--probe-at', required=True)
    ap.add_argument('--source', required=True); ap.add_argument('--reader-fingerprint', required=True)
    a = ap.parse_args()
    emit(a.log, a.receipt, a.remote_bytes, a.remote_sha256, a.probe_at,
         json.loads(Path(a.source).read_text()), a.reader_fingerprint)


if __name__ == '__main__':
    main()
