{{/* Names are capped at 40 characters, so "<fullname>-train-<target>" stays within a CronJob's 52. */}}
{{- define "pa.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 40 | trimSuffix "-" -}}
{{- end -}}

{{- define "pa.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "pa.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "pa.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels for one component: include "pa.selector" (list . "operator") */}}
{{- define "pa.selector" -}}
{{- $ := index . 0 -}}
app.kubernetes.io/name: {{ include "pa.name" $ }}
app.kubernetes.io/instance: {{ $.Release.Name }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end -}}

{{/* An image reference, the digest preferred: include "pa.image" (list . .Values.images.operator) */}}
{{- define "pa.image" -}}
{{- $ := index . 0 -}}
{{- $img := index . 1 -}}
{{- if $img.digest -}}
{{- printf "%s@%s" $img.repository $img.digest -}}
{{- else -}}
{{- printf "%s:%s" $img.repository (default $.Chart.AppVersion $img.tag) -}}
{{- end -}}
{{- end -}}

{{- define "pa.forecasterPort" -}}
{{- ternary 8443 8000 .Values.forecaster.auth.enabled -}}
{{- end -}}

{{- define "pa.forecasterURL" -}}
{{- $scheme := ternary "https" "http" .Values.forecaster.auth.enabled -}}
{{- with .Values.operator.forecasterAddress -}}
{{- $host := index (splitList ":" .) 0 -}}
{{- if and $.Values.forecaster.auth.enabled (not (has $host $.Values.forecaster.tls.extraDNSNames)) -}}
{{- fail (printf "operator.forecasterAddress host %q must be in forecaster.tls.extraDNSNames (certificate verification stays on)" $host) -}}
{{- end -}}
{{- printf "%s://%s" $scheme . -}}
{{- else -}}
{{- printf "%s://%s-forecaster.%s.svc:%s" $scheme (include "pa.fullname" .) .Release.Namespace (include "pa.forecasterPort" .) -}}
{{- end -}}
{{- end -}}

{{- define "pa.forecasterTLSSecret" -}}
{{- if eq .Values.forecaster.tls.source "existingSecret" -}}
{{- required "forecaster.tls.existingSecret is required with tls.source existingSecret" .Values.forecaster.tls.existingSecret -}}
{{- else -}}
{{- printf "%s-forecaster-tls" (include "pa.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "pa.operatorSubject" -}}
{{- printf "system:serviceaccount:%s:%s-operator" .Release.Namespace (include "pa.fullname" .) -}}
{{- end -}}

{{- define "pa.stateClaim" -}}
{{- default (printf "%s-forecast-state" (include "pa.fullname" .)) .Values.forecaster.statistical.persistence.existingClaim -}}
{{- end -}}

{{- define "pa.statistical" -}}
{{- if eq .Values.forecaster.engine "statistical" }}true{{ end -}}
{{- end -}}

{{/* "true" when a lab qualification is mounted (C2.8 lab evaluation only); it exists only for the statistical engine. */}}
{{- define "pa.labQualification" -}}
{{- if .Values.forecaster.statistical.labQualification.configMap -}}
{{- if not (include "pa.statistical" .) }}{{ fail "forecaster.statistical.labQualification needs forecaster.engine: statistical" }}{{ end -}}
{{- if not .Values.images.statisticalForecaster.digest }}{{ fail "forecaster.statistical.labQualification needs images.statisticalForecaster.digest (the evaluation image the qualification names)" }}{{ end -}}
true
{{- end -}}
{{- end -}}

{{/*
"true" when a demonstration qualification and its contract are mounted (a scoped live demonstration only: not a
release and not an evaluation qualification); it exists only for the statistical engine, only with the image pinned by
digest, and never beside a lab qualification.
*/}}
{{- define "pa.demonstrationQualification" -}}
{{- if .Values.forecaster.statistical.demonstrationQualification.configMap -}}
{{- if .Values.forecaster.statistical.labQualification.configMap }}{{ fail "forecaster.statistical.demonstrationQualification and forecaster.statistical.labQualification are both set: at most one of the two documents" }}{{ end -}}
{{- if not (include "pa.statistical" .) }}{{ fail "forecaster.statistical.demonstrationQualification needs forecaster.engine: statistical" }}{{ end -}}
{{- if not .Values.images.statisticalForecaster.digest }}{{ fail "forecaster.statistical.demonstrationQualification needs images.statisticalForecaster.digest (the evaluation image the qualification names)" }}{{ end -}}
{{- if eq .Values.forecaster.statistical.demonstrationQualification.key .Values.forecaster.statistical.demonstrationQualification.contractKey }}{{ fail "forecaster.statistical.demonstrationQualification.key and contractKey must be two different keys (the qualification and the contract it names)" }}{{ end -}}
true
{{- end -}}
{{- end -}}

{{/* A binary quantity (Ki, Mi, Gi or Ti; decimals allowed) in bytes. */}}
{{- define "pa.quantityBytes" -}}
{{- $unit := substr (sub (len .) 2 | int) (len .) . -}}
{{- mulf (trimSuffix $unit . | float64) (get (dict "Ki" 1024 "Mi" 1048576 "Gi" 1073741824 "Ti" 1099511627776) $unit) | floor | int64 -}}
{{- end -}}

{{/* The statistical journal's byte budget from forecaster.statistical.stateQuota (Mi or Gi). It must leave a fifth of
     the chart's own volume for the fit artifacts and the filesystem; an existingClaim's capacity is not known here. */}}
{{- define "pa.stateQuotaBytes" -}}
{{- $q := .Values.forecaster.statistical.stateQuota -}}
{{- $bytes := include "pa.quantityBytes" $q | int64 -}}
{{- $p := .Values.forecaster.statistical.persistence -}}
{{- if and (not $p.existingClaim) (gt (mul $bytes 5) (mul (include "pa.quantityBytes" $p.size | int64) 4)) -}}
{{- fail (printf "forecaster.statistical.stateQuota (%s) must be at most 80%% of forecaster.statistical.persistence.size (%s): the volume also holds the fit artifacts, and a journal that fills it stops the forecaster" $q $p.size) -}}
{{- end -}}
{{- $bytes -}}
{{- end -}}

{{- define "pa.modelsClaim" -}}
{{- default (printf "%s-models" (include "pa.fullname" .)) .Values.forecaster.persistence.existingClaim -}}
{{- end -}}

{{/* The pod and container security context of every component (uid: 65532 operator, 10001 Python). */}}
{{- define "pa.podSecurity" -}}
runAsNonRoot: true
runAsUser: {{ . }}
runAsGroup: {{ . }}
fsGroup: {{ . }}
fsGroupChangePolicy: OnRootMismatch
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "pa.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}

{{/* Writable home, temporary and cache directories for the Python components, all on the /tmp emptyDir, set before
     any import (TensorFlow/Keras write under HOME and the XDG directories). */}}
{{- define "pa.pythonEnv" -}}
- name: HOME
  value: /tmp/home
- name: TMPDIR
  value: /tmp
- name: XDG_CACHE_HOME
  value: /tmp/.cache
- name: XDG_CONFIG_HOME
  value: /tmp/.config
- name: KERAS_HOME
  value: /tmp/.keras
- name: PYTHONDONTWRITEBYTECODE
  value: "1"
- name: PYTHONUNBUFFERED
  value: "1"
{{- if not (include "pa.statistical" .) }}
- name: MODEL_DIR
  value: /models
{{- end }}
{{- end -}}

{{/* NetworkPolicy egress rules every component needs: DNS, the Kubernetes API, Prometheus (built as data, then YAML).
With kubeAPI.cilium the API rule is left out: the CiliumNetworkPolicy in networkpolicies.yaml allows it instead. */}}
{{- define "pa.commonEgress" -}}
{{- $np := .Values.networkPolicy -}}
{{- $dns := dict "to" (list (dict "namespaceSelector" $np.dns.namespaceSelector "podSelector" $np.dns.podSelector)) "ports" (list (dict "protocol" "UDP" "port" 53) (dict "protocol" "TCP" "port" 53)) -}}
{{- $apiPorts := list -}}
{{- range $np.kubeAPI.ports }}{{ $apiPorts = append $apiPorts (dict "protocol" "TCP" "port" .) }}{{ end -}}
{{- $api := dict "ports" $apiPorts -}}
{{- if $np.kubeAPI.cidrs -}}
{{- $to := list -}}
{{- range $np.kubeAPI.cidrs }}{{ $to = append $to (dict "ipBlock" (dict "cidr" .)) }}{{ end -}}
{{- $_ := set $api "to" $to -}}
{{- end -}}
{{- if $np.kubeAPI.cilium -}}
{{- if $np.kubeAPI.cidrs }}{{ fail "networkPolicy.kubeAPI.cidrs must be empty when networkPolicy.kubeAPI.cilium is true (the CiliumNetworkPolicy names the kube-apiserver entity)" }}{{ end -}}
{{- toYaml (list $dns (include "pa.prometheusRule" . | fromYaml)) -}}
{{- else -}}
{{- toYaml (list $dns $api (include "pa.prometheusRule" . | fromYaml)) -}}
{{- end -}}
{{- end -}}

{{- define "pa.prometheusRule" -}}
{{- $p := .Values.networkPolicy.prometheus -}}
{{- if not (or $p.namespaceSelector $p.podSelector $p.ipBlocks) -}}
{{- fail "networkPolicy.prometheus needs a namespaceSelector, podSelector or ipBlocks when networkPolicy.enabled (or set networkPolicy.enabled=false)" -}}
{{- end -}}
{{- $to := list -}}
{{- if or $p.namespaceSelector $p.podSelector -}}
{{- $peer := dict -}}
{{- with $p.namespaceSelector }}{{ $_ := set $peer "namespaceSelector" . }}{{ end -}}
{{- with $p.podSelector }}{{ $_ := set $peer "podSelector" . }}{{ end -}}
{{- $to = append $to $peer -}}
{{- end -}}
{{- range $p.ipBlocks }}{{ $to = append $to (dict "ipBlock" (dict "cidr" .)) }}{{ end -}}
{{- $ports := list -}}
{{- range $p.ports }}{{ $ports = append $ports (dict "protocol" "TCP" "port" .) }}{{ end -}}
{{- toYaml (dict "to" $to "ports" $ports) -}}
{{- end -}}

{{/* The alert rules (one group), shared by the PrometheusRule and the VMRule. Every selector is scoped to this
installation (its scrape job and pa_install_namespace, set by the scrape objects in metrics.yaml), and every alert
carries the release and its namespace. */}}
{{- define "pa.alertGroups" -}}
{{- $a := .Values.metrics.alerts -}}
{{- $ns := .Release.Namespace -}}
{{- $op := printf "job=%q, pa_install_namespace=%q" (printf "%s-operator-metrics" (include "pa.fullname" .)) $ns -}}
{{- $fc := printf "job=%q, pa_install_namespace=%q" (printf "%s-forecaster" (include "pa.fullname" .)) $ns -}}
{{- $journal := printf "max(predictive_autoscaler_forecaster_journal_unhealthy_since_seconds{%s})" $fc -}}
{{- $cond := printf "max by (namespace, autoscaler) (predictive_autoscaler_condition{%s, type=%%q, status=%%q})" $op -}}
{{- $cutoff := printf "predictive_autoscaler_model_training_cutoff_timestamp_seconds{%s}" $op -}}
{{- $issued := printf "predictive_autoscaler_forecast_issued_timestamp_seconds{%s}" $op -}}
{{- $base := dict "pa_release" .Release.Name "pa_release_namespace" $ns -}}
{{- /* No forecast used (forecasting enabled), split by why: a reactive fallback, or a hold for a missing capacity. */ -}}
{{- $noForecast := printf "((%s == 0) unless on (namespace, autoscaler) (max by (namespace, autoscaler) (predictive_autoscaler_forecasting_enabled{%s}) == 0))" (printf $cond "ForecastAvailable" "True") $op -}}
{{- $noCapacity := printf $cond "CapacityConfigured" "False" -}}
{{- $rules := list
  (dict "alert" "PredictiveAutoscalerNotReady" "for" $a.notReadyFor "severity" "warning"
        "expr" (printf "%s == 0" (printf $cond "Ready" "True"))
        "summary" "{{ $labels.namespace }}/{{ $labels.autoscaler }}: reconcile failing"
        "description" "Ready has not been True for the alert's duration; see the autoscaler's conditions and events.")
  (dict "alert" "PredictiveAutoscalerTelemetryUnavailable" "for" $a.telemetryFor "severity" "warning"
        "expr" (printf "%s == 0" (printf $cond "TelemetryAvailable" "True"))
        "summary" "{{ $labels.namespace }}/{{ $labels.autoscaler }}: request rate unavailable, replica count held"
        "description" "The operator cannot measure the target's request rate and holds the current replica count.")
  (dict "alert" "PredictiveAutoscalerForecastUnavailable" "for" $a.forecastFor "severity" "info"
        "expr" (printf "label_replace(%s unless on (namespace, autoscaler) (%s == 1), \"fallback\", \"reactive\", \"\", \"\") or label_replace(%s and on (namespace, autoscaler) (%s == 1), \"fallback\", \"capacity_hold\", \"\", \"\")" $noForecast $noCapacity $noForecast $noCapacity)
        "summary" "{{ $labels.namespace }}/{{ $labels.autoscaler }}: no forecast used"
        "description" "{{ if eq $labels.fallback \"capacity_hold\" }}No per-replica capacity is configured (CapacityConfigured=False, reason CapacityMissing): no forecast is requested and the replica count is held, not scaled on the measured rate. Set spec.metrics.requests.capacityPerReplica.{{ else }}Decisions use the measured rate only. Expected until the first training has enough history.{{ end }} Autoscalers with spec.prediction.enabled=false never alert.")
  (dict "alert" "PredictiveAutoscalerConflict" "for" $a.conflictFor "severity" "warning"
        "expr" (printf "%s == 1" (printf $cond "ConflictDetected" "True"))
        "summary" "{{ $labels.namespace }}/{{ $labels.autoscaler }}: another replica writer targets the Deployment"
        "description" "The operator writes nothing while another writer (an HPA, KEDA, another autoscaler) exists.")
  (dict "alert" "PredictiveAutoscalerModelStale" "for" "15m" "severity" "warning"
        "expr" (printf "max by (namespace, application) ((time() - %s > %d) and (%s > 0) and (time() - %s < 3600))" $cutoff (mul (int $a.modelStaleHours) 3600) $cutoff $issued)
        "summary" "{{ $labels.namespace }}/{{ $labels.application }}: served model is stale"
        "description" (printf "Forecasts in the last hour came from a model whose training cutoff is older than %d h; check the training CronJob." (int $a.modelStaleHours)))
  (dict "alert" "PredictiveAutoscalerOperatorDown" "for" $a.absentFor "severity" "critical"
        "expr" (printf "absent(up{%s} == 1)" $op)
        "summary" "predictive-autoscaler operator not scraped"
        "description" "No operator scrape target is up. A healthy scrape proves only that the endpoint answers, not that reconciles succeed (see PredictiveAutoscalerNotReady).")
  (dict "alert" "PredictiveAutoscalerForecasterDown" "for" $a.absentFor "severity" "warning"
        "expr" (printf "absent(up{%s} == 1)" $fc)
        "summary" "predictive-autoscaler forecaster not scraped"
        "description" "No forecaster scrape target is up; the operator falls back to the measured rate.")
  (dict "alert" "PredictiveAutoscalerForecastJournalUnhealthy" "for" $a.journalFor "severity" "warning"
        "expr" (printf "%s > 0" $journal)
        "summary" "predictive-autoscaler statistical forecaster: journal unhealthy"
        "description" "The statistical forecaster's journal cannot be written or trusted, so it answers 503 and is not ready: autoscalers with engine statistical are reactive only. It retries by itself: 5 s after the failure, then 15 s, 1 min, 5 min and 15 min after each failed attempt (about 5 s, 20 s, 80 s, 6 min and 21 min after the failure), then every 30 min. Still firing: its /ready names the reason; a full state volume or a corrupt journal needs an operator (docs/helm.md).")
-}}
- name: predictive-autoscaler
  rules:
{{- range $rules }}
    - alert: {{ .alert }}
      expr: {{ .expr | quote }}
      for: {{ .for }}
      labels: {{- merge (dict "severity" .severity) $base ($a.labels | default dict) | toYaml | nindent 8 }}
      annotations:
        summary: {{ .summary | quote }}
        description: {{ .description | quote }}
{{- end }}
{{- end -}}
