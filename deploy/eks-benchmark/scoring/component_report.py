"""Recorded hybrid/pattern comparisons, conditional on accepted issued targets.

This is not attempted-origin coverage or a formal winner/reliability decision.
It consumes the scorer's existing actual rows and performs no network requests.
"""
from collections import Counter
from datetime import datetime, timezone
import math


def instant(value):
    # Match score.parse_ts for core identities, including historical naive inputs.
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return parsed.astimezone(timezone.utc)


def input_window(record):
    value = record.get('inference_input_end')
    if not isinstance(value, (str, datetime)):
        return None
    try:
        return instant(value).isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def finite(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def components(record):
    """Validate diagnostics without changing acceptance of the core forecast."""
    c = record.get('components')
    if c is None:
        return None, 'absent', set()
    if not isinstance(c, dict):
        return None, 'invalid', set()
    if c.get('schema') != 'component-v1':
        return None, 'schema', set()
    status = c.get('status')
    if status != 'ok':
        return None, status if status in ('absent', 'invalid', 'misaligned') else 'invalid', set()
    fields = ('target_timestamps', 'pattern', 'lstm', 'blended', 'final',
              'pattern_available_per_step', 'network_finite_per_step', 'pattern_weights')
    forecasts = record['forecasts']
    if len(forecasts) != 6 or any(not isinstance(c.get(k), list) or len(c[k]) != 6 for k in fields):
        return None, 'length', set()
    if ('network_failed' not in c or 'pattern_source' not in c or 'reason' not in c
            or c['reason'] is not None
            or any(c[k] is not None and not isinstance(c[k], str) for k in ('network_failed', 'pattern_source'))):
        return None, 'invalid', set()
    mismatch = set()
    for i, fc in enumerate(forecasts):
        try:
            aligned = fc['step'] == i + 1 and instant(c['target_timestamps'][i]) == instant(fc['target_at'])
        except (ValueError, TypeError, AttributeError):
            aligned = False
        if not aligned:
            return None, 'misaligned', set()
        if not finite(c['final'][i]) or not finite(fc['rpm']) or round(c['final'][i], 2) != fc['rpm']:
            return None, 'final_mismatch', set()
        for key in ('pattern', 'lstm', 'blended'):
            if c[key][i] is not None and not finite(c[key][i]):
                return None, 'invalid', set()
        for key, flag in (('pattern', 'pattern_available_per_step'), ('lstm', 'network_finite_per_step')):
            if type(c[flag][i]) is not bool or c[flag][i] != finite(c[key][i]):
                return None, 'invalid', set()
        weight = c['pattern_weights'][i]
        if not finite(weight) or not 0 <= weight <= 1:
            return None, 'invalid', set()
        if all(finite(c[k][i]) for k in ('pattern', 'lstm', 'blended')):
            expected = weight * c['pattern'][i] + (1 - weight) * c['lstm'][i]
            if not math.isclose(expected, c['blended'][i], rel_tol=1e-9, abs_tol=1e-9):
                mismatch.add(i)
    return c, None, mismatch


def row_key(row):
    return (instant(row['issued_at']), int(row['step']), instant(row['target_at']),
            row.get('artifact_sha256') or 'unknown')


def per_step(targets):
    """Availability uses matured issued steps; errors use the finite paired set."""
    out = {}
    for step in sorted(set(range(1, 7)) | {r['step'] for r in targets}):
        selected = [r for r in targets if r['step'] == step]
        matured = [r for r in selected if not r['outstanding']]
        paired = [r for r in matured if all(finite(r[k]) for k in ('actual', 'hybrid', 'pattern'))]
        n = len(matured)
        patterns = sum(finite(r['pattern']) for r in matured)
        metrics = {}
        for name in ('hybrid', 'pattern'):
            errors = [r[name] - r['actual'] for r in paired]
            # Overflow is not a numeric result. Availability remains a forecast property.
            usable = bool(errors) and all(math.isfinite(e) for e in errors)
            metrics[name] = {'MAE_rpm': math.fsum(abs(e) / len(errors) for e in errors) if usable else None,
                             'signed_bias_rpm': math.fsum(e / len(errors) for e in errors) if usable else None}
        out[str(step)] = {
            'issued_steps_matured': n, 'outstanding': len(selected) - n,
            'actual_available': sum(finite(r['actual']) for r in matured),
            'hybrid_finite': sum(finite(r['hybrid']) for r in matured),
            'pattern_usable': patterns, 'network_finite': sum(finite(r['network']) for r in matured),
            'pattern_available_fraction_of_issued': patterns / n if n else None,
            'pattern_unavailable_reasons': dict(Counter(r['pattern_reason'] for r in matured if not finite(r['pattern']))),
            'paired_n': len(paired), 'paired': metrics,
            'paired_loss_nonfinite': sum(any(not math.isfinite(r[k] - r['actual']) for k in ('hybrid', 'pattern')) for r in paired),
            'unique_paired_target_times': len({instant(r['target_at']) for r in paired}),
            'unique_paired_input_windows': len({r['inference_input_end'] for r in paired if r['inference_input_end'] is not None}),
            'paired_input_window_unknown': sum(r['inference_input_end'] is None for r in paired),
            'blend_identity_mismatch': sum(r['blend_identity_mismatch'] for r in matured),
        }
    return out


def summarize_components(accepted, actual_rows, as_of=None):
    """Join within one app/namespace; never substitute another arm or previous-day query."""
    if len({(r.get('application'), r.get('namespace')) for r in accepted}) > 1:
        raise ValueError('Component report requires application/namespace-filtered issuances')
    actuals = {row_key(row): row['actual'] for row in actual_rows}
    issued_keys = [row_key({'issued_at': r['issued_at'], 'artifact_sha256': r.get('artifact_sha256'),
                            'step': fc['step'], 'target_at': fc['target_at']})
                   for r in accepted for fc in r['forecasts']]
    duplicates = {'issued': len(issued_keys) - len(set(issued_keys)),
                  'actual_rows': len(actual_rows) - len(actuals)}
    if any(duplicates.values()):
        return {'schema': 'recorded-components-v1', 'status': 'invalid',
                'reason': 'duplicate_target_identity', 'duplicate_identity': duplicates,
                'formal_comparison_complete': False,
                'scope': 'Ambiguous component join; raw scoring is unchanged. No paired result available.',
                'issuances_accepted': len(accepted), 'per_step': {}, 'per_artifact': {}, 'targets': []}
    targets = []
    for record in accepted:
        c, reason, mismatches = components(record)
        for i, fc in enumerate(record['forecasts']):
            target = instant(fc['target_at'])
            row = dict(issued_at=record['issued_at'], step=int(fc['step']), target_at=fc['target_at'],
                       artifact_sha256=record.get('artifact_sha256') or 'unknown')
            outstanding = as_of is not None and target > as_of
            actual = actuals.get(row_key(row)) if not outstanding else None
            pattern = c['pattern'][i] if c else None
            row.update(inference_input_end=input_window(record),
                       target_anchor=record.get('target_anchor'),
                       effective_lead_seconds=(target - instant(record['issued_at'])).total_seconds(),
                       outstanding=outstanding, actual=actual if finite(actual) else None,
                       hybrid=fc['rpm'] if finite(fc['rpm']) else None,
                       pattern=pattern, network=c['lstm'][i] if c else None,
                       pattern_reason=reason or ('unavailable' if pattern is None else None),
                       component_reason=reason, blend_identity_mismatch=i in mismatches)
            targets.append(row)
    return {
        'schema': 'recorded-components-v1', 'status': 'ok', 'formal_comparison_complete': False,
        'scope': 'Conditional on matured accepted issued targets; attempted/scheduled origins are not measured here. Not protocol availability or a winner decision.',
        'pairing': 'Recorded served hybrid versus recorded seasonal component at the identical canonical actual; not the previous-day query or a separate live arm.',
        'independence': 'Shared input windows and target observations are counted explicitly. No confidence interval or pooled improvement is reported.',
        'blend_identity_check': 'Non-fatal diagnostic when both inputs and blend are finite; rel_tol=1e-9, abs_tol=1e-9. Final may differ from pre-adjustment blend.',
        'issuances_accepted': len(accepted), 'per_step': per_step(targets),
        'per_artifact': {artifact: {'per_step': per_step([r for r in targets if r['artifact_sha256'] == artifact])}
                         for artifact in sorted({r['artifact_sha256'] for r in targets})},
        'targets': targets,
    }
