{{- define "okg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "okg.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "okg.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "okg.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "okg.labels" -}}
helm.sh/chart: {{ include "okg.chart" . }}
app.kubernetes.io/name: {{ include "okg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "okg.selectorLabels" -}}
app.kubernetes.io/name: {{ include "okg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "okg.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "okg.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- /*
  The runtime image. A digest pins it; otherwise the tag. With neither set, a
  released chart (Chart.yaml annotation okg.mitdbg.io/released: "true", which
  the release packaging sets) uses the image tagged with its appVersion. A
  source checkout is not released: no image exists for its appVersion, so it
  still needs a tag or a digest. The schema refuses a tag and a digest together.
*/ -}}
{{- define "okg.runtimeImage" -}}
{{- if and .Values.images.okg.digest .Values.images.okg.tag -}}
{{- fail "images.okg: set a tag or a digest, not both" -}}
{{- end -}}
{{- if .Values.images.okg.digest -}}
{{- printf "%s@%s" .Values.images.okg.repository .Values.images.okg.digest -}}
{{- else if .Values.images.okg.tag -}}
{{- printf "%s:%s" .Values.images.okg.repository .Values.images.okg.tag -}}
{{- else if eq (index (.Chart.Annotations | default dict) "okg.mitdbg.io/released" | default "") "true" -}}
{{- printf "%s:%s" .Values.images.okg.repository (required "images.okg.tag is empty and the released chart has no appVersion" .Chart.AppVersion) -}}
{{- else -}}
{{- fail "images.okg.tag is required when digest is empty: this chart is not a released chart (Chart.yaml annotation okg.mitdbg.io/released is not \"true\"), so no image is published for its appVersion" -}}
{{- end -}}
{{- end }}

{{- define "okg.imagePullSecrets" -}}
{{- with .Values.images.pullSecrets }}
imagePullSecrets:
  {{- range . }}
  - name: {{ . }}
  {{- end }}
{{- end }}
{{- end }}

{{- /*
  The PostgreSQL image, chosen the same way as the runtime image: a digest, else
  the tag, else, in a released chart only, the image tagged with its
  appVersion. Each release publishes both images under that one tag. A source
  checkout has no published image for its appVersion and refuses to render.
*/ -}}
{{- define "okg.postgresImage" -}}
{{- if and .Values.postgresql.image.digest .Values.postgresql.image.tag -}}
{{- fail "postgresql.image: set a tag or a digest, not both" -}}
{{- end -}}
{{- if .Values.postgresql.image.digest -}}
{{- printf "%s@%s" .Values.postgresql.image.repository .Values.postgresql.image.digest -}}
{{- else if .Values.postgresql.image.tag -}}
{{- printf "%s:%s" .Values.postgresql.image.repository .Values.postgresql.image.tag -}}
{{- else if eq (index (.Chart.Annotations | default dict) "okg.mitdbg.io/released" | default "") "true" -}}
{{- printf "%s:%s" .Values.postgresql.image.repository (required "postgresql.image.tag is empty and the released chart has no appVersion" .Chart.AppVersion) -}}
{{- else -}}
{{- fail "postgresql.image.tag is required when digest is empty: this chart is not a released chart (Chart.yaml annotation okg.mitdbg.io/released is not \"true\"), so no image is published for its appVersion" -}}
{{- end -}}
{{- end }}

{{- define "okg.commonEnv" -}}
{{- /*
  The deployment manifest is baked into the image (`COPY . .`), so the pod reads
  the SAME deployment.yaml as the host checkout. A static runtime.supervisor in
  that file cannot be truthful for both targets: okg-workspace runs as a host
  launchd worker AND as this container. This declares the adapter for THIS
  instantiation. It is a declaration, not inference -- the chart's values are the
  deployment configuration for this target -- which is why the code is forbidden
  from sniffing /.dockerenv or platform.system() instead.
*/ -}}
- name: OKG_RUNTIME_SUPERVISOR
  valueFrom:
    configMapKeyRef:
      name: {{ include "okg.fullname" . }}
      key: runtime-supervisor
- name: OKG_DEPLOYMENT
  valueFrom:
    configMapKeyRef:
      name: {{ include "okg.fullname" . }}
      key: deployment
- name: OKG_REPO_ROOT
  valueFrom:
    configMapKeyRef:
      name: {{ include "okg.fullname" . }}
      key: repo-root
{{- if .Values.repositorySync.enabled }}
- name: OKG_DEPLOYMENTS_DIR
  valueFrom:
    configMapKeyRef:
      name: {{ include "okg.fullname" . }}
      key: deployments-dir
{{- end }}
- name: OKG_ENVIRONMENT_DATA_ROOT
  valueFrom:
    configMapKeyRef:
      name: {{ include "okg.fullname" . }}
      key: environment-data-root
- name: OKG_RUNTIME_ENVIRONMENT
  valueFrom:
    configMapKeyRef:
      name: {{ include "okg.fullname" . }}
      key: runtime-environment
{{- end }}

{{- define "okg.runtimeSubjectEnv" -}}
- name: OKG_NOMOS_SUBJECT
  valueFrom:
    configMapKeyRef:
      name: {{ include "okg.fullname" . }}
      key: runtime-nomos-subject
{{- end }}

{{- define "okg.mcpSubjectEnv" -}}
- name: OKG_NOMOS_SUBJECT
  valueFrom:
    configMapKeyRef:
      name: {{ include "okg.fullname" . }}
      key: mcp-nomos-subject
{{- end }}

{{- define "okg.extraSecretEnvFor" -}}
{{- $root := index . 0 -}}
{{- $target := index . 1 -}}
{{- range $entry := $root.Values.secrets.extraEnv }}
{{- if has $target $entry.targets }}
- name: {{ $entry.name }}
  valueFrom:
    secretKeyRef:
      name: {{ $root.Values.secrets.existingSecret }}
      key: {{ $entry.key }}
{{- end }}
{{- end }}
{{- end }}

{{- /*
  Plain environment variables (extraEnv) for one named container. Values are
  not secret; credentials go through secrets.extraEnv.
*/ -}}
{{- define "okg.extraEnvFor" -}}
{{- $root := index . 0 -}}
{{- $target := index . 1 -}}
{{- range $entry := $root.Values.extraEnv }}
{{- if has $target $entry.targets }}
- name: {{ $entry.name }}
  value: {{ $entry.value | quote }}
{{- end }}
{{- end }}
{{- end }}

{{- /*
  Git credentials for private source hosts. One credential.https://<host>.helper
  per host through GIT_CONFIG_COUNT/KEY/VALUE (git 2.31 or newer). The helper
  answers only `get` and prints the token from GIT_CRED_TOKEN_<i>, which a
  secretKeyRef fills, so the token is never written to disk, a URL or argv.
  The schema restricts host, username and secretKey to characters that cannot
  break out of the helper's shell text.
*/ -}}
{{- define "okg.gitCredentialsEnv" -}}
{{- $seenHosts := dict }}
{{- range .Values.gitCredentials.hosts }}
{{- if hasKey $seenHosts .host }}
{{- fail (printf "gitCredentials.hosts lists host %s twice; Git would use only one of its credentials" .host) }}
{{- end }}
{{- $_ := set $seenHosts .host true }}
{{- end }}
{{- with .Values.gitCredentials.hosts }}
- name: GIT_TERMINAL_PROMPT
  value: "0"
- name: GIT_CONFIG_COUNT
  value: {{ len . | quote }}
{{- range $i, $h := . }}
- name: GIT_CONFIG_KEY_{{ $i }}
  value: {{ printf "credential.https://%s.helper" $h.host | quote }}
- name: GIT_CONFIG_VALUE_{{ $i }}
  value: {{ printf "!f() { test \"$1\" = get || exit 0; echo username=%s; echo \"password=$GIT_CRED_TOKEN_%d\"; }; f" $h.username $i | quote }}
- name: GIT_CRED_TOKEN_{{ $i }}
  valueFrom:
    secretKeyRef:
      name: {{ $.Values.secrets.existingSecret }}
      key: {{ $h.secretKey }}
{{- end }}
{{- end }}
{{- end }}

{{- define "okg.writerDsnEnv" -}}
- name: OKG_DSN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecret }}
      key: {{ .Values.secrets.externalDsnKey }}
{{- /* Named by okg's one-line error for an unreachable database (#3582). */}}
- name: OKG_DSN_SOURCE
  value: {{ printf "Secret %s, key %s" .Values.secrets.existingSecret .Values.secrets.externalDsnKey | quote }}
{{- if ne .Values.okg.deploymentDsnEnv "OKG_DSN" }}
- name: {{ .Values.okg.deploymentDsnEnv }}
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecret }}
      key: {{ .Values.secrets.externalDsnKey }}
{{- end }}
{{- end }}

{{- define "okg.readerDsnEnv" -}}
- name: OKG_DSN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecret }}
      key: {{ .Values.secrets.mcpDsnKey }}
- name: OKG_DSN_SOURCE
  value: {{ printf "Secret %s, key %s" .Values.secrets.existingSecret .Values.secrets.mcpDsnKey | quote }}
- name: OKG_MCP_RO_DSN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecret }}
      key: {{ .Values.secrets.mcpDsnKey }}
{{- if ne .Values.okg.deploymentDsnEnv "OKG_DSN" }}
- name: {{ .Values.okg.deploymentDsnEnv }}
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecret }}
      key: {{ .Values.secrets.mcpDsnKey }}
{{- end }}
{{- end }}

{{- define "okg.containerSecurityContext" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end }}
