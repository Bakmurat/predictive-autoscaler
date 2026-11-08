"""Values frozen by EVAL-C28-MANIFEST (sections 3-8 and 11). Nothing here is tuned from results; a change is a new
manifest version and a new confirmation cohort. Each constant names the section it comes from."""

ARMS = ("H-CPU", "H-RPS", "PA-R", "PA-N", "PA-E")                      # section 4
PA_ARMS = ("PA-R", "PA-N", "PA-E")
CANDIDATE_ARMS = ("PA-N", "PA-E")                                       # the forecasting arms under test
PROFILES = {"s20": 20, "s90": 90}                                       # nominal creation-to-Ready seconds (section 3)
PROFILE_P90_BANDS = {"s20": (20.0, 30.0), "s90": (90.0, 120.0)}         # admission bands for measured p90
PROFILE_STARTUP_BUDGET = {"s20": 30, "s90": 120}                        # configured minimum/fallback (section 6)

CONFIRMATION_SEEDS = (201, 202, 203, 204, 205)                          # section 5
TUNING_SEEDS = (101, 102, 103, 104, 105)                                # section 4
ORDER_SEED = 20261106                                                   # arm-session order (section 5)
BOOTSTRAP_SEED = 2808                                                   # section 7
BOOTSTRAP_RESAMPLES = 10_000
FAMILY_ALPHA = 0.05                                                     # Holm family-wise (section 7)

MIN_REPLICAS, MAX_REPLICAS = 1, 12                                      # section 3
CAPACITY_RATIO = 0.875                                                  # c = 0.875 c0 (section 3)
NOMINAL_COVERAGE_BPS = 9000                                             # 90 % (section 4)
MISCOVERAGE_BPS = 1000
CALIBRATION_MIN, CALIBRATION_MAX, RETENTION_DAYS = 128, 512, 7
EXTRA_MAX_REPLICAS, EXTRA_MAX_PERCENT = 2, 50
SAMPLE_INTERVAL_S, SETTLEMENT_S, GRACE_S = 60, 120, 600                 # section 3
HORIZON_MINUTES = 60
DECISION_INTERVAL_S = 60
ACTUATION_ALLOWANCE_S = 15                                              # section 6 (only if measured delays allow)
RATE_RANGE = "60s"                                                      # RPS query range (section 3)
SCRAPE_INTERVAL_S = 15
REQUEST_TIMEOUT_S = 2                                                   # section 3; no automatic retries

# Common scaling budgets (section 4): scale-up stabilization 0; scale-down stabilization 300 s; at most
# max(10 % of current, 2 pods) per 120 s, selectPolicy Max. Scale-down headroom 0 for every PA arm.
SCALE_DOWN_STABILIZATION_S = 300
SCALE_DOWN_PERIOD_S = 120
SCALE_DOWN_PERCENT, SCALE_DOWN_PODS = 10, 2
PA_HEADROOM_PERCENT = 0
HPA_CPU_CANDIDATES = (40, 50, 60, 70, 80)

# Service SLO per one-minute bin (section 7).
SLO_P95_S, SLO_P99_S, SLO_ERROR_FRACTION = 0.200, 0.500, 0.001

# The 24-hour daily shape u = offered_RPS / c through (hour, u) (section 5); +-5 % seeded minute perturbation.
DAILY_SHAPE = ((0, 2), (6, 2), (9, 8), (12, 8), (15, 5), (18, 9), (21, 3), (24, 2))
PERTURBATION = 0.05
MAX_DEMAND_U = 10                                                       # except the capacity-blocked scenario

# Scored-day windows (section 5): id, start hour, end hour, kind. Kinds: ordinary, step, ramp, holiday,
# metric_outage, forecast_outage, quota, slow_start. The manifest's table omits 03:00-06:00; it is the ordinary 2c low
# load with no injection, so it is an explicit ordinary window here and the windows tile the whole day.
WINDOWS = (
    ("w00-low", 0, 1, "ordinary"),
    ("w01-step", 1, 3, "step"),
    ("w03-low", 3, 6, "ordinary"),
    ("w06-ramp", 6, 9, "ramp"),
    ("w09-holiday", 9, 12, "holiday"),
    ("w12-metric-outage", 12, 14, "metric_outage"),
    ("w14-forecast-outage", 14, 16, "forecast_outage"),
    ("w16-quota", 16, 18, "quota"),
    ("w18-high", 18, 20, "ordinary"),
    ("w20-slow-start", 20, 22, "slow_start"),
    ("w22-decline", 22, 24, "ordinary"),
)
PREDICTABLE_WINDOWS = ("w06-ramp", "w00-low", "w03-low", "w18-high", "w22-decline")   # "predictable ramp/daily windows"
ORDINARY_WINDOWS = ("w00-low", "w03-low", "w18-high", "w22-decline")    # non-injected (G5 absolute SLO, 99 % aggregate)
ROBUSTNESS_WINDOWS = ("w01-step", "w09-holiday", "w12-metric-outage", "w14-forecast-outage", "w20-slow-start")
QUOTA_WINDOW = "w16-quota"

# Section 8 numeric bars.
VALIDITY = {"recorder_coverage": 0.99, "max_transition_gap_s": 30.0, "dispatch_fraction": 0.995,
            "timestamp_skew_s": 1.0}
NI_SLO_MINUTES_PER_DAY = 24.0                                           # G5: added SLO-violation minutes/day
NI_ERROR_FRACTION = 0.001                                               # 0.1 percentage point
ORDINARY_SLO_MINUTE_SHARE = 0.99
BENEFIT_SLO_REDUCTION = 0.20                                            # route A
BENEFIT_REPLICA_SECONDS_REDUCTION = 0.10                                # route B (comparator violation time zero)
ROBUSTNESS_MINUTES_PER_HOUR = 1.0
SURPLUS_MATCH_RELATIVE = 0.05                                           # section 7 matching
SURPLUS_MATCH_FLOOR_REPLICAS = 0.05
NI_COMPARATORS = ("H-RPS", "PA-R")                                      # G5 preselected comparators
BENEFIT_COMPARATORS = ("H-RPS", "PA-R")                                 # Holm across these
ROBUSTNESS_COMPARATOR = "PA-R"
MIN_SCORED_DAYS = 5                                                     # per arm per profile
MAX_INFRA_REPLACEMENTS = 2

OUTCOMES = ("PASS", "FAIL", "INCONCLUSIVE", "INCOMPLETE", "NOT READY")

# The section 1 prerequisites the study must record, every one a strict boolean. rps_metric_fault: the target-local
# request-metric (RPS query) outage of 12:15-12:30 for H-RPS, PA-R, PA-N and PA-E, implemented and verified (section
# 12.1: not covered by the H-CPU exception). The former cpu_metric_fault blocker is replaced by the section 12
# applicability declaration below, never by a fabricated cpu_metric_fault=true.
PREREQUISITES = ("pinned_tree", "replay_simulator", "lab_qualification", "family_restriction", "policy_cost_binding",
                 "envelope_and_soak", "recorder_frozen", "rps_metric_fault")

# Section 12 (amendment of 2026-11-07): scenario-by-arm applicability of the 12:15-12:30 metric outage. H-CPU's
# CPU-metric outage is NOT TESTED (no shared metrics-server/kubelet/firewall intervention): H-CPU runs the full
# 24-hour program with normal CPU telemetry, its 12:00-14:00 window an unperturbed control (kept, never zero-filled,
# never counted as fault coverage). The four request-metric arms keep the outage as REQUIRED. The run-lock binds this
# exact declaration to the amended manifest's digest; the study and the scorer accept nothing else (no waiver).
METRIC_OUTAGE_APPLICABILITY = {"H-CPU": "not_tested", "H-RPS": "required", "PA-R": "required", "PA-N": "required",
                               "PA-E": "required"}
APPLICABILITY = {"amendment": "EVAL-C28-MANIFEST section 12 (2026-11-07)",
                 "metric_outage": METRIC_OUTAGE_APPLICABILITY}
METRIC_OUTAGE_EVIDENCE = ("verified", "failed", "missing")            # a required arm's per-block record
CPU_OUTAGE_DISCLOSURE = ("CPU HPA metric-outage behavior was not tested; its corresponding interval retained normal "
                         "CPU telemetry. Outage comparisons concern request-metric controllers only.")

# spec.prediction.families per PA arm (C2.8a, PR #27) and the candidate set it permits: PA-N never forecasts or
# selects E1-product; PA-R and PA-E carry both (PA-R's forecasts never add replicas: predictive extra 0).
PA_FAMILIES = {"PA-R": ("seasonalNaive", "statistical"), "PA-N": ("seasonalNaive",),
               "PA-E": ("seasonalNaive", "statistical")}
PA_CANDIDATES = {"PA-R": ("seasonal_naive", "statistical"), "PA-N": ("seasonal_naive",),
                 "PA-E": ("seasonal_naive", "statistical")}
GATES = ("fallback", "blocked_slow_startup", "quota_guards", "quota_observable", "quota_resumed", "g4_qualified")

# The queries reduce needs (the run-lock must define each; the collector runs exactly the locked definitions).
REQUIRED_QUERIES = ("offered_rps", "responses_per_minute", "latency_buckets_per_minute", "ready_replicas",
                    "pod_replicas", "ingress_admitted_per_minute", "k6_requests_per_minute", "k6_dropped_per_minute")
RECONCILE_TOLERANCE = 0.01          # per-window |scheduled - (dropped + dispatched)| / scheduled before a window is flagged


def window_hours(window_id: str) -> float:
    for wid, start, end, _ in WINDOWS:
        if wid == window_id:
            return float(end - start)
    raise KeyError(window_id)
