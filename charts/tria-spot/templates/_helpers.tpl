{{- define "tria-spot.tenant" -}}
{{- required "spot.tenant is required when spot.enabled is true" .Values.spot.tenant -}}
{{- end -}}

{{- define "tria-spot.name" -}}
{{- default (printf "%s-spot" (include "tria-spot.tenant" .)) .Values.spot.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tria-spot.saName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "tria-spot.name" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create is false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "tria-spot.labels" -}}
app.kubernetes.io/name: tria-spot
app.kubernetes.io/instance: {{ include "tria-spot.name" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "tria-spot.selectorLabels" -}}
app.kubernetes.io/name: tria-spot
app.kubernetes.io/instance: {{ include "tria-spot.name" . }}
{{- end -}}

{{- define "tria-spot.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
