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
{{- /* A tag of digits only (a short git sha can be) is read as a number and
       would render as %!s(float64=...): refuse it instead of an unstartable pod. */ -}}
{{- if or (kindIs "float64" .Values.image.tag) (kindIs "int64" .Values.image.tag) -}}
{{- fail "image.tag must be a string: quote it in the values file or use --set-string image.tag=..." -}}
{{- end -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "tria-spot.coreIdentity" -}}
{{- if not (has .Values.core.identity (list "serviceAccount" "secret" "certificate")) -}}
{{- fail "core.identity must be serviceAccount, secret or certificate" -}}
{{- end -}}
{{- .Values.core.identity -}}
{{- end -}}

{{- /* Egress peers for one NetworkPolicy target (.t, the values block; .what, its
       name for the error). A selector that is empty is left out: an empty
       namespaceSelector would match every namespace. */ -}}
{{- define "tria-spot.egressPeers" -}}
{{- $t := .t -}}
{{- $peers := list -}}
{{- if or $t.namespaceSelector $t.podSelector -}}
{{- $p := dict -}}
{{- if $t.namespaceSelector }}{{ $_ := set $p "namespaceSelector" $t.namespaceSelector }}{{ end -}}
{{- if $t.podSelector }}{{ $_ := set $p "podSelector" $t.podSelector }}{{ end -}}
{{- $peers = append $peers $p -}}
{{- end -}}
{{- range $t.cidrs -}}
{{- $peers = append $peers (dict "ipBlock" (dict "cidr" .)) -}}
{{- end -}}
{{- if not $peers -}}
{{- fail (printf "networkPolicy.%s needs a namespaceSelector/podSelector or cidrs so egress to it is explicit" .what) -}}
{{- end -}}
{{- toYaml $peers -}}
{{- end -}}

{{- /* Where the spot runs: a pod (deployment) or an actor of Agent Substrate
       (substrate; the core's ADR-107). */ -}}
{{- define "tria-spot.runtime" -}}
{{- $r := default "deployment" .Values.runtime -}}
{{- if not (has $r (list "deployment" "substrate")) -}}
{{- fail "runtime must be deployment or substrate" -}}
{{- end -}}
{{- $r -}}
{{- end -}}

{{- /* One egress destination of a substrate actor, from a URL: a dict with the
       scheme, the host name and the port. A bare host:port is read as http. */ -}}
{{- define "tria-spot.destination" -}}
{{- $u := urlParse . -}}
{{- $scheme := $u.scheme -}}
{{- if not $scheme }}{{ fail (printf "%q is not a URL with a scheme" .) }}{{ end -}}
{{- if not (has $scheme (list "http" "https")) }}{{ fail (printf "%q: only http and https URLs can be reached from an actor" .) }}{{ end -}}
{{- $port := default (ternary "443" "80" (eq $scheme "https")) (trimPrefix ":" (regexFind ":[0-9]+$" $u.host)) -}}
{{- dict "scheme" $scheme "host" $u.hostname "port" (int $port) | toJson -}}
{{- end -}}
