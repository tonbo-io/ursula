{{- define "ursula.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ursula.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 61 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 61 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 61 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "ursula.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ursula.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ursula.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ursula.labels" -}}
helm.sh/chart: {{ include "ursula.chart" . }}
{{ include "ursula.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "ursula.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "ursula.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "ursula.headlessServiceName" -}}
{{- $base := include "ursula.fullname" . | trunc 54 | trimSuffix "-" -}}
{{- printf "%s-headless" $base | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ursula.s3SecretName" -}}
{{- if .Values.s3.credentials.existingSecret -}}
{{- .Values.s3.credentials.existingSecret -}}
{{- else -}}
{{- printf "%s-s3" (include "ursula.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "ursula.hasInlineS3Credentials" -}}
{{- or .Values.s3.credentials.accessKeyId .Values.s3.credentials.secretAccessKey .Values.s3.credentials.sessionToken -}}
{{- end -}}

{{- define "ursula.hasS3Credentials" -}}
{{- or .Values.s3.credentials.existingSecret (include "ursula.hasInlineS3Credentials" .) -}}
{{- end -}}

{{- define "ursula.joinPath" -}}
{{- $parts := list -}}
{{- range . -}}
{{- $part := . | toString | trimAll "/" -}}
{{- if $part -}}
{{- $parts = append $parts $part -}}
{{- end -}}
{{- end -}}
{{- join "/" $parts -}}
{{- end -}}

{{- define "ursula.image" -}}
{{- $tag := default .Chart.AppVersion .Values.global.image.tag -}}
{{- printf "%s:%s" .Values.global.image.repository $tag -}}
{{- end -}}

{{- define "ursula.testImage" -}}
{{- $repository := default .Values.global.image.repository .Values.tests.image.repository -}}
{{- $tag := default (default .Chart.AppVersion .Values.global.image.tag) .Values.tests.image.tag -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

{{- define "ursula.testPullPolicy" -}}
{{- default .Values.global.image.pullPolicy .Values.tests.image.pullPolicy -}}
{{- end -}}

{{- define "ursula.gatewayName" -}}
{{- $base := include "ursula.name" . | trunc 55 | trimSuffix "-" -}}
{{- printf "%s-gateway" $base -}}
{{- end -}}

{{- define "ursula.gatewayFullname" -}}
{{- $base := include "ursula.fullname" . | trunc 55 | trimSuffix "-" -}}
{{- printf "%s-gateway" $base -}}
{{- end -}}

{{- define "ursula.gatewaySelectorLabels" -}}
app.kubernetes.io/name: {{ include "ursula.gatewayName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ursula.gatewayLabels" -}}
helm.sh/chart: {{ include "ursula.chart" . }}
{{ include "ursula.gatewaySelectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "ursula.gatewayImage" -}}
{{- $repository := default .Values.global.image.repository .Values.gateway.image.repository -}}
{{- $tag := default (default .Chart.AppVersion .Values.global.image.tag) .Values.gateway.image.tag -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

{{/*
Validate a DNS-1123 label that is rendered into pod names or peer DNS.
*/}}
{{- define "ursula.validateDnsLabel" -}}
{{- $name := .name | toString -}}
{{- $value := .value | toString -}}
{{- $valueLen := $value | len -}}
{{- $pattern := "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" -}}
{{- $isEmpty := $value | eq "" -}}
{{- $isTooLong := gt $valueLen 63 -}}
{{- $isDnsLabel := regexMatch $pattern $value -}}
{{- if or $isEmpty $isTooLong ($isDnsLabel | not) -}}
{{- fail (printf "%s must be a non-empty DNS-1123 label with at most 63 characters; got %q" $name $value) -}}
{{- end -}}
{{- end -}}

{{/*
Validate a DNS name that is rendered into peer FQDNs.
*/}}
{{- define "ursula.validateDnsName" -}}
{{- $name := .name | toString -}}
{{- $value := .value | toString -}}
{{- $valueLen := $value | len -}}
{{- $pattern := "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$" -}}
{{- $isEmpty := $value | eq "" -}}
{{- $isTooLong := gt $valueLen 253 -}}
{{- $isDnsName := regexMatch $pattern $value -}}
{{- if or $isEmpty $isTooLong ($isDnsName | not) -}}
{{- fail (printf "%s must be a non-empty DNS name with at most 253 characters; got %q" $name $value) -}}
{{- end -}}
{{- end -}}

{{/*
Validate component resource names to prevent collisions.
*/}}
{{- define "ursula.validateComponentNames" -}}
{{- $serverName := include "ursula.fullname" . -}}
{{- $gatewayName := include "ursula.gatewayFullname" . -}}
{{- include "ursula.validateDnsLabel" (dict "name" "server fullname" "value" $serverName) -}}
{{- include "ursula.validateDnsLabel" (dict "name" "gateway fullname" "value" $gatewayName) -}}
{{- if eq $serverName $gatewayName -}}
{{- fail (printf "gateway resource name %q must be distinct from server resource name" $gatewayName) -}}
{{- end -}}
{{- end -}}

{{/*
Validate values consumed by the generated entrypoint before manifests are
accepted. The entrypoint should only guard runtime-derived pod ordinal state.
*/}}
{{- define "ursula.validateEntrypointConfig" -}}
{{- $fullname := include "ursula.fullname" . -}}
{{- $headless := include "ursula.headlessServiceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $contract := .Values.deploymentContract -}}
{{- $serverServiceAccountName := include "ursula.serviceAccountName" . -}}
{{- $serverRoleArn := index .Values.serviceAccount.annotations "eks.amazonaws.com/role-arn" | default "" | toString -}}
{{- $replicaCount := .Values.server.replicaCount | int -}}
{{- $coreCount := .Values.server.coreCount | int -}}
{{- $clientPort := .Values.server.ports.client | int -}}
{{- $clusterDomain := .Values.global.clusterDomain | toString -}}
{{- $raftGroupCount := .Values.raft.groupCount | int -}}
{{- $logDir := .Values.raft.logDir | toString -}}
{{- include "ursula.validateDnsLabel" (dict "name" "Release.Namespace" "value" $namespace) -}}
{{- include "ursula.validateDnsLabel" (dict "name" "fullname" "value" $fullname) -}}
{{- include "ursula.validateDnsLabel" (dict "name" "headlessServiceName" "value" $headless) -}}
{{- include "ursula.validateDnsName" (dict "name" "clusterDomain" "value" $clusterDomain) -}}
{{- if and $contract.expectedNamespace (ne ($contract.expectedNamespace | toString) $namespace) -}}
{{- fail (printf "deploymentContract expected namespace %q, but Helm is rendering namespace %q" $contract.expectedNamespace $namespace) -}}
{{- end -}}
{{- if and $contract.serverServiceAccountName (ne ($contract.serverServiceAccountName | toString) $serverServiceAccountName) -}}
{{- fail (printf "deploymentContract expected server ServiceAccount %q, but chart resolves %q" $contract.serverServiceAccountName $serverServiceAccountName) -}}
{{- end -}}
{{- if and $contract.serverRoleArn (ne ($contract.serverRoleArn | toString) $serverRoleArn) -}}
{{- fail "deploymentContract serverRoleArn does not match serviceAccount.annotations[eks.amazonaws.com/role-arn]" -}}
{{- end -}}
{{- if and $contract.serverS3Prefix (ne ($contract.serverS3Prefix | toString) (.Values.s3.prefix | toString)) -}}
{{- fail (printf "deploymentContract expected server S3 prefix %q, but s3.prefix is %q" $contract.serverS3Prefix .Values.s3.prefix) -}}
{{- end -}}
{{- range $i := until $replicaCount -}}
{{- $podName := printf "%s-%d" $fullname $i -}}
{{- include "ursula.validateDnsLabel" (dict "name" "generated pod DNS label" "value" $podName) -}}
{{- end -}}
{{- if lt $replicaCount 1 -}}
{{- fail (printf "server.replicaCount must be at least 1; got %v" .Values.server.replicaCount) -}}
{{- end -}}
{{- if eq $replicaCount 2 -}}
{{- fail "server.replicaCount=2 tolerates no node loss: every group and the meta group need both nodes. Run 1, or 3 and more" -}}
{{- end -}}
{{- if lt $coreCount 1 -}}
{{- fail (printf "server.coreCount must be at least 1; got %v" .Values.server.coreCount) -}}
{{- end -}}
{{- if lt $raftGroupCount 1 -}}
{{- fail (printf "raft.groupCount must be at least 1; got %v" .Values.raft.groupCount) -}}
{{- end -}}
{{- if or (lt $clientPort 1) (gt $clientPort 65535) -}}
{{- fail (printf "server.ports.client must be between 1 and 65535; got %v" .Values.server.ports.client) -}}
{{- end -}}
{{- $adminPort := regexFind "[0-9]+$" (.Values.server.adminListen | toString) | int -}}
{{- if eq $clientPort $adminPort -}}
{{- fail (printf "server.ports.client must differ from the server.adminListen port; both are %d" $clientPort) -}}
{{- end -}}
{{- if .Values.server.networkPolicy.enabled -}}
{{- $adminHost := regexReplaceAll ":[0-9]+$" (.Values.server.adminListen | toString) "" -}}
{{- if not (or (hasPrefix "127." $adminHost) (eq $adminHost "localhost")) -}}
{{- fail (printf "server.networkPolicy.enabled admits nothing to the admin plane, but server.adminListen (%s) is not on loopback: keep it on loopback, or leave the policy off and restrict the admin port another way" .Values.server.adminListen) -}}
{{- end -}}
{{- end -}}
{{- if .Values.server.clusterListen -}}
{{- $clusterPort := .Values.server.ports.cluster | int -}}
{{- if or (lt $clusterPort 1) (gt $clusterPort 65535) -}}
{{- fail (printf "server.ports.cluster must be between 1 and 65535; got %v" .Values.server.ports.cluster) -}}
{{- end -}}
{{- if eq $clusterPort $clientPort -}}
{{- fail (printf "server.ports.cluster must differ from server.ports.client; both are %d" $clientPort) -}}
{{- end -}}
{{- if eq $clusterPort $adminPort -}}
{{- fail (printf "server.ports.cluster must differ from the server.adminListen port; both are %d" $adminPort) -}}
{{- end -}}
{{- end -}}
{{- if $logDir | eq "" -}}
{{- fail "raft.logDir must be non-empty" -}}
{{- end -}}
{{- $walFsync := .Values.raft.walFsync | default "" | toString -}}
{{- if or ($walFsync | eq "") ($walFsync | eq "always") ($walFsync | eq "never") | not -}}
{{- fail (printf "raft.walFsync must be empty, always or never; got %q" .Values.raft.walFsync) -}}
{{- end -}}
{{- if and (gt (.Values.raft.minAvailableBytes | int64) 0) (le (.Values.raft.resumeAvailableBytes | int64) (.Values.raft.minAvailableBytes | int64)) -}}
{{- fail "raft.resumeAvailableBytes must exceed raft.minAvailableBytes when disk-pressure admission is enabled" -}}
{{- end -}}
{{- $serverUsesS3 := or .Values.coldStorage.enabled (.Values.snapshotStore.backend | eq "s3") -}}
{{- if $serverUsesS3 -}}
{{- if eq (.Values.s3.bucket | toString | trim) "" -}}
{{- fail "s3.bucket must be set when coldStorage.enabled=true or snapshotStore.backend=s3" -}}
{{- end -}}
{{- if and .Values.s3.credentials.existingSecret (include "ursula.hasInlineS3Credentials" .) -}}
{{- fail "s3.credentials.existingSecret cannot be set together with inline S3 credentials" -}}
{{- end -}}
{{- if and (include "ursula.hasInlineS3Credentials" .) (or (.Values.s3.credentials.accessKeyId | not) (.Values.s3.credentials.secretAccessKey | not)) -}}
{{- fail "s3.credentials.accessKeyId and s3.credentials.secretAccessKey must both be set when inline S3 credentials are configured" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Validate Ingress values. Keep failures in chart rendering instead of letting
Kubernetes reject the rendered Ingress later.
*/}}
{{- define "ursula.validateIngressConfig" -}}
{{- if and .Values.gateway.ingress.enabled (not .Values.gateway.enabled) -}}
{{- fail "gateway.ingress.enabled requires gateway.enabled=true" -}}
{{- end -}}
{{- if .Values.gateway.ingress.enabled -}}
{{- $hosts := .Values.gateway.ingress.hosts -}}
{{- if eq (len $hosts) 0 -}}
{{- fail "gateway.ingress.hosts must contain at least one host when gateway.ingress.enabled=true" -}}
{{- end -}}
{{- range $hostIndex, $host := $hosts -}}
{{- if eq ($host.host | toString | trim) "" -}}
{{- fail (printf "gateway.ingress.hosts[%d].host must be non-empty when gateway.ingress.enabled=true" $hostIndex) -}}
{{- end -}}
{{- $paths := $host.paths -}}
{{- if eq (len $paths) 0 -}}
{{- fail (printf "gateway.ingress.hosts[%d].paths must contain at least one path when gateway.ingress.enabled=true" $hostIndex) -}}
{{- end -}}
{{- range $pathIndex, $path := $paths -}}
{{- if eq ($path.path | toString | trim) "" -}}
{{- fail (printf "gateway.ingress.hosts[%d].paths[%d].path must be non-empty" $hostIndex $pathIndex) -}}
{{- end -}}
{{- if not (hasPrefix "/" ($path.path | toString)) -}}
{{- fail (printf "gateway.ingress.hosts[%d].paths[%d].path must start with '/'" $hostIndex $pathIndex) -}}
{{- end -}}
{{- $pathType := $path.pathType | toString -}}
{{- if not (has $pathType (list "Prefix" "Exact" "ImplementationSpecific")) -}}
{{- fail (printf "gateway.ingress.hosts[%d].paths[%d].pathType must be Prefix, Exact, or ImplementationSpecific; got %q" $hostIndex $pathIndex $pathType) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range $tlsIndex, $tls := .Values.gateway.ingress.tls -}}
{{- if eq ($tls.secretName | toString | trim) "" -}}
{{- fail (printf "gateway.ingress.tls[%d].secretName must be non-empty when gateway.ingress.tls is configured" $tlsIndex) -}}
{{- end -}}
{{- if eq (len $tls.hosts) 0 -}}
{{- fail (printf "gateway.ingress.tls[%d].hosts must contain at least one host" $tlsIndex) -}}
{{- end -}}
{{- range $tlsHostIndex, $tlsHost := $tls.hosts -}}
{{- if eq ($tlsHost | toString | trim) "" -}}
{{- fail (printf "gateway.ingress.tls[%d].hosts[%d] must be non-empty" $tlsIndex $tlsHostIndex) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Validate gateway values before manifests are accepted.
*/}}
{{- define "ursula.validateGatewayConfig" -}}
{{- include "ursula.validateComponentNames" . -}}
{{- if .Values.gateway.enabled -}}
{{- $gatewayPort := .Values.gateway.ports.http | int -}}
{{- $maxRequestBodyBytes := .Values.gateway.maxRequestBodyBytes | int64 -}}
{{- $gracefulShutdownTimeoutSeconds := .Values.gateway.gracefulShutdownTimeoutSeconds | int -}}
{{- if or (lt $gatewayPort 1) (gt $gatewayPort 65535) -}}
{{- fail (printf "gateway.ports.http must be between 1 and 65535; got %v" .Values.gateway.ports.http) -}}
{{- end -}}
{{- if lt $maxRequestBodyBytes 1 -}}
{{- fail (printf "gateway.maxRequestBodyBytes must be at least 1; got %v" .Values.gateway.maxRequestBodyBytes) -}}
{{- end -}}
{{- /* The protocol's request limit, which every node applies: a gateway
above it exits at startup. */ -}}
{{- if gt $maxRequestBodyBytes 33554432 -}}
{{- fail (printf "gateway.maxRequestBodyBytes must be at most 33554432, the protocol's request limit; got %d" $maxRequestBodyBytes) -}}
{{- end -}}
{{- /* An unset budget grows to hold a body at the limit, so only a set one
is checked: the gateway exits at startup unless a body at the limit fits the
part of the budget large bodies may take, the budget less the small-body
headroom (32 MiB unless extraArgs sets --small-body-headroom-bytes, which the
gateway then checks alone). */ -}}
{{- $headroomOverridden := false -}}
{{- range .Values.gateway.extraArgs -}}
{{- if hasPrefix "--small-body-headroom-bytes" (toString .) -}}
{{- $headroomOverridden = true -}}
{{- end -}}
{{- end -}}
{{- if and (not $headroomOverridden) (not (eq .Values.gateway.bodyBudgetBytes nil)) -}}
{{- $budgetBytes := .Values.gateway.bodyBudgetBytes | int64 -}}
{{- $sharedBytes := sub $budgetBytes 33554432 -}}
{{- if gt $maxRequestBodyBytes $sharedBytes -}}
{{- fail (printf "gateway.maxRequestBodyBytes (%d) must fit the gateway's body budget less its 32 MiB small-body headroom (%d): set gateway.bodyBudgetBytes to at least %d" $maxRequestBodyBytes $sharedBytes (add $maxRequestBodyBytes 33554432)) -}}
{{- end -}}
{{- end -}}
{{- if lt $gracefulShutdownTimeoutSeconds 1 -}}
{{- fail (printf "gateway.gracefulShutdownTimeoutSeconds must be at least 1; got %v" .Values.gateway.gracefulShutdownTimeoutSeconds) -}}
{{- end -}}
{{- range $label := list "app.kubernetes.io/name" "app.kubernetes.io/instance" -}}
{{- if hasKey $.Values.gateway.podLabels $label -}}
{{- fail (printf "gateway.podLabels must not set reserved selector label %q" $label) -}}
{{- end -}}
{{- end -}}
{{- if and .Values.gateway.autoscaling.enabled (not .Values.gateway.autoscaling.targetCPUUtilizationPercentage) (not .Values.gateway.autoscaling.targetMemoryUtilizationPercentage) -}}
{{- fail "gateway.autoscaling requires at least one target utilization metric when gateway.autoscaling.enabled=true" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Selector labels of the Helm test pod. Its name label differs from the server
pods', so the release label does not put it behind the server Services.
*/}}
{{- define "ursula.testSelectorLabels" -}}
app.kubernetes.io/name: {{ include "ursula.name" . | trunc 58 | trimSuffix "-" }}-test
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: helm-test
{{- end -}}

{{/*
The port peers dial for the cluster plane: server.ports.cluster when the
cluster plane listens apart (server.clusterListen), else server.ports.client.
*/}}
{{- define "ursula.clusterPort" -}}
{{- if .Values.server.clusterListen -}}
{{- .Values.server.ports.cluster | int -}}
{{- else -}}
{{- .Values.server.ports.client | int -}}
{{- end -}}
{{- end -}}

{{/*
The ursulactl manifest of the server pods: node id ordinal + 1, at the peer
host. http_url is the Raft address. ursulactl reads metrics, readiness and
placement at client_url.
*/}}
{{- define "ursula.clusterManifest" -}}
{{- $fullname := include "ursula.fullname" . -}}
{{- $headless := include "ursula.headlessServiceName" . -}}
{{- $clientPort := .Values.server.ports.client | int -}}
{{- $clusterPort := include "ursula.clusterPort" . | int -}}
{{- $nodes := list -}}
{{- range $i := until (.Values.server.replicaCount | int) -}}
{{- $podName := printf "%s-%d" $fullname $i -}}
{{- $host := printf "%s.%s.%s.svc.%s" $podName $headless $.Release.Namespace ($.Values.global.clusterDomain | toString) -}}
{{- $nodes = append $nodes (dict "id" (add1 $i) "http_url" (printf "http://%s:%d" $host $clusterPort) "client_url" (printf "http://%s:%d" $host $clientPort) "host" $host "name" $podName) -}}
{{- end -}}
{{- dict "nodes" $nodes | toPrettyJson -}}
{{- end -}}

{{/*
Selector labels of the scale-check hook's pod, which the NetworkPolicy
admits to the client plane as it admits the Helm test pod.
*/}}
{{- define "ursula.scaleCheckSelectorLabels" -}}
app.kubernetes.io/name: {{ include "ursula.name" . | trunc 50 | trimSuffix "-" }}-scale-check
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: scale-check
{{- end -}}
