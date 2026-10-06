{{- define "flowbridge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "flowbridge.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "flowbridge.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- define "flowbridge.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: {{ include "flowbridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "flowbridge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flowbridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "flowbridge.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}
{{- define "flowbridge.allowedHosts" -}}
{{- $name := include "flowbridge.fullname" . -}}
{{- $hosts := list (printf "%s:8790" $name) (printf "%s.%s:8790" $name .Release.Namespace) (printf "%s.%s.svc:8790" $name .Release.Namespace) (printf "%s.%s.svc.%s:8790" $name .Release.Namespace .Values.clusterDomain) -}}
{{- if .Values.ingress.enabled -}}
{{- $hosts = append $hosts .Values.ingress.host -}}
{{- $hosts = append $hosts (printf "%s:443" .Values.ingress.host) -}}
{{- end -}}
{{- if .Values.route.enabled -}}
{{- $hosts = append $hosts .Values.route.host -}}
{{- $hosts = append $hosts (printf "%s:443" .Values.route.host) -}}
{{- end -}}
{{- join "," $hosts -}}
{{- end -}}
{{- define "flowbridge.allowedOrigins" -}}
{{- $origins := list -}}
{{- if .Values.ingress.enabled -}}
{{- $origins = append $origins (printf "https://%s" .Values.ingress.host) -}}
{{- end -}}
{{- if .Values.route.enabled -}}
{{- $origins = append $origins (printf "https://%s" .Values.route.host) -}}
{{- end -}}
{{- join "," $origins -}}
{{- end -}}
{{- define "flowbridge.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
capabilities:
  drop:
    - ALL
seccompProfile:
  type: RuntimeDefault
{{- end -}}
{{- define "flowbridge.validate" -}}
{{- if .Values.live.enabled -}}
{{- if ne (int .Values.replicaCount) 1 -}}
{{- fail "Live migration requires replicaCount=1." -}}
{{- end -}}
{{- if or (empty .Values.live.tokenSecretName) (empty .Values.live.storage.existingClaim) -}}
{{- fail "Live migration requires an existing token Secret and persistent volume claim." -}}
{{- end -}}
{{- end -}}
{{- if and .Values.ingress.enabled .Values.route.enabled -}}
{{- fail "Enable either ingress or route, not both." -}}
{{- end -}}
{{- if and .Values.ingress.enabled (or (empty .Values.ingress.host) (empty .Values.ingress.tlsSecretName)) -}}
{{- fail "Ingress requires an explicit host and a TLS secret name." -}}
{{- end -}}
{{- if and .Values.route.enabled (empty .Values.route.host) -}}
{{- fail "OpenShift Route requires an explicit host for the application's host/origin allowlist." -}}
{{- end -}}
{{- end -}}
