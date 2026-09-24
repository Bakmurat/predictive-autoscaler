"""Transfer an already probed append-only prefix with bounded, verified retries."""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

CHUNK = 1024 * 1024
# Copy one bounded chunk before hashing/streaming it. The live file may append
# during the copy; the last chunk is capped at the original probed prefix.
CHUNK_SCRIPT = rf'''
set -eu
part=$(mktemp /tmp/forecast-chunk.XXXXXX)
trap 'rm -f "$part"' EXIT
dd if="$1" of="$part" bs={CHUNK} skip="$2" count=1
[ "$(wc -c < "$part")" -ge "$3" ]
printf '%s\n' "$3"
head -c "$3" "$part" | sha256sum
head -c "$3" "$part"
'''


def reader_fingerprint(directory):
    """Versioned ordered fingerprint; pre-v2 receipts hash only the shell file."""
    names = ('read-forecast-log.sh', 'forecast_transfer.py')
    parts = ['v2'] + [hashlib.sha256((directory/name).read_bytes()).hexdigest() for name in names]
    return hashlib.sha256(('\n'.join(parts) + '\n').encode()).hexdigest()


def identity(raw):
    pod = json.loads(raw)
    states = pod['status']['containerStatuses']
    uid = pod['metadata']['uid']
    if not uid or len(states) != 1 or states[0]['name'] != 'reader':
        raise ValueError('unexpected reader identity')
    return uid, states[0]['restartCount']


def transfer(context, namespace, pod, source, size, expected_sha, output, diagnostics, expected_identity):
    if size <= 0 or not re.fullmatch('[0-9a-f]{64}', expected_sha):
        raise ValueError('invalid remote prefix fingerprint')
    deadline = time.monotonic() + 900
    wall_deadline = time.time() + 900
    command = ['kubectl', '--context', context, '--request-timeout=30s']
    identity_reads = 0
    with (diagnostics/'attempts.jsonl').open('x') as log:
        def record(**fields):
            log.write(json.dumps({'at': dt.datetime.now(dt.timezone.utc).isoformat(), **fields}) + '\n')
            log.flush()

        def remaining():
            value = min(deadline-time.monotonic(), wall_deadline-time.time())
            if value <= 0:
                raise TimeoutError('forecast transfer deadline exceeded')
            return value

        def run(args, stderr_path):
            timeout = min(45, remaining())
            try:
                result = subprocess.run(command + args, capture_output=True, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                stderr_path.write_bytes(exc.stderr or b'')
                raise
            stderr_path.write_bytes(result.stderr)
            return result

        def check_identity():
            nonlocal identity_reads
            identity_reads += 1
            result = run(['get', 'pod', pod, '-n', namespace, '-o', 'json'],
                         diagnostics/f'identity-{identity_reads}.stderr')
            if result.returncode or identity(result.stdout) != expected_identity:
                raise ValueError('reader identity changed or unavailable')

        record(event='start', bytes=size, sha256=expected_sha, chunk_bytes=CHUNK,
               reader_identity=expected_identity)
        try:
            check_identity()
            digest = hashlib.sha256()
            with output.open('xb') as dest:
                for index, offset in enumerate(range(0, size, CHUNK)):
                    length = min(CHUNK, size-offset)
                    for attempt in range(1, 4):
                        started = time.monotonic()
                        wall_started = time.time()
                        stderr = diagnostics/f'chunk-{index}-{attempt}.stderr'
                        code = None
                        try:
                            result = run(['exec', '-n', namespace, pod, '--', 'sh', '-c',
                                          CHUNK_SCRIPT, 'forecast-chunk', source, str(index), str(length)], stderr)
                            code = result.returncode
                            if code:
                                raise ValueError('exec exited ' + str(code))
                            count, sha_line, body = result.stdout.split(b'\n', 2)
                            sha = sha_line.split()[0].decode('ascii')
                            if not count.isdigit() or int(count) != length or len(body) != length:
                                raise ValueError('chunk length mismatch')
                            if not re.fullmatch('[0-9a-f]{64}', sha) or hashlib.sha256(body).hexdigest() != sha:
                                raise ValueError('chunk hash mismatch')
                            remaining()
                        except (ValueError, IndexError, UnicodeError, subprocess.TimeoutExpired) as exc:
                            record(event='retry', index=index, offset=offset, attempt=attempt,
                                   exit_code=code, stderr=stderr.name, error=str(exc),
                                   elapsed_seconds=time.monotonic()-started, wall_seconds=time.time()-wall_started)
                            check_identity()
                            if attempt == 3:
                                raise ValueError('chunk retry budget exhausted') from exc
                            continue
                        dest.write(body)
                        digest.update(body)
                        record(event='chunk', index=index, offset=offset, attempt=attempt,
                               exit_code=code, stderr=stderr.name, bytes=len(body), sha256=sha,
                               elapsed_seconds=time.monotonic()-started, wall_seconds=time.time()-wall_started)
                        break
            check_identity()
            if output.stat().st_size != size or digest.hexdigest() != expected_sha:
                raise ValueError('whole prefix integrity mismatch')
            remaining()
            record(event='complete', bytes=size, sha256=digest.hexdigest())
        except Exception as exc:
            record(event='failed', error=str(exc))
            raise


def main():
    if sys.argv[1:2] == ['--fingerprint']:
        print(reader_fingerprint(Path(sys.argv[2])))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('context', 'namespace', 'pod', 'source', 'size', 'sha', 'output', 'diagnostics', 'identity'):
        parser.add_argument(name)
    args = parser.parse_args()
    try:
        transfer(args.context, args.namespace, args.pod, args.source, int(args.size), args.sha,
                 Path(args.output), Path(args.diagnostics), identity(Path(args.identity).read_bytes()))
    except (OSError, ValueError, KeyError, IndexError, subprocess.TimeoutExpired) as exc:
        print('COLLECTION FAILED: ' + str(exc) + '; diagnostics: ' + args.diagnostics, file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
