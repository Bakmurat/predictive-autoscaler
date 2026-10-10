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

{{- define "pa.forecasterURL" -}}
{{- printf "http://%s-forecaster.%s.svc:8000" (include "pa.fullname" .) .Release.Namespace -}}
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
- name: MODEL_DIR
  value: /models
{{- end -}}

{{/* NetworkPolicy egress rules every component needs: DNS, the Kubernetes API, Prometheus (built as data, then YAML). */}}
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
{{- toYaml (list $dns $api (include "pa.prometheusRule" . | fromYaml)) -}}
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
