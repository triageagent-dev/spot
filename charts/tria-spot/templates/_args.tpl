{{- /* The flags both runtimes pass, from the model to the tasks. Written as a YAML
       list. A substrate actor mounts no Secret, so it gets no token file: what
       ego needs is put on the request by the egress policy. */ -}}
{{- define "tria-spot.agentArgs" -}}
{{- $llm := .Values.llm -}}
{{- $substrate := eq (include "tria-spot.runtime" .) "substrate" -}}
{{- if $llm.model }}
- --model={{ $llm.model }}
{{- end }}
- --effort={{ $llm.effort }}
- --max-steps={{ int $llm.maxSteps }}
{{- if $llm.maxToolCalls }}
- --max-tool-calls={{ int $llm.maxToolCalls }}
{{- end }}
{{- if hasKey $llm "maxTurnTokens" }}
- --max-turn-tokens={{ int64 $llm.maxTurnTokens }}
{{- end }}
- --state-dir=/state
- --instruction-cell={{ .Values.agent.instructionCell }}
- --min-wake={{ .Values.agent.minWake }}
- --max-wake={{ .Values.agent.maxWake }}
- --poll-interval={{ .Values.agent.pollInterval }}
- --edge-cooldown={{ .Values.agent.edgeCooldown }}
- --core-tools={{ .Values.agent.coreTools }}
- --chat-batch={{ .Values.agent.chatBatch }}
{{- if .Values.agent.notesOnly }}
- --notes-only
{{- end }}
{{- if not .Values.agent.issueWakes }}
- --issue-wakes=false
{{- end }}
{{- if and (hasKey .Values.agent "fastLane") (not .Values.agent.fastLane) }}
- --fast-lane=false
{{- end }}
{{- if ne (int (default 8 .Values.agent.fastMaxToolCalls)) 8 }}
- --fast-max-tool-calls={{ int .Values.agent.fastMaxToolCalls }}
{{- end }}
{{- if ne (int (default 120000 .Values.agent.fastMaxTurnTokens)) 120000 }}
- --fast-max-turn-tokens={{ int .Values.agent.fastMaxTurnTokens }}
{{- end }}
- --max-edge-wakes-per-hour={{ int .Values.agent.maxEdgeWakesPerHour }}
- --turn-timeout={{ .Values.agent.turnTimeout }}
- --tool-timeout={{ .Values.agent.toolTimeout }}
- --tool-result-max-bytes={{ int .Values.agent.toolResultMaxBytes }}
- --daily-token-ceiling={{ int64 .Values.agent.dailyTokenCeiling }}
- --hourly-token-ceiling={{ int64 (dig "agent" "hourlyTokenCeiling" 600000 .Values.AsMap) }}
- --skip-quiet-wakes={{ dig "agent" "skipQuietWakes" true .Values.AsMap }}
{{- if .Values.telemetry.enabled }}
- --otel-endpoint={{ required "telemetry.endpoint is required when telemetry.enabled is true" .Values.telemetry.endpoint }}
{{- if .Values.telemetry.tenant }}
- --otel-tenant={{ .Values.telemetry.tenant }}
{{- end }}
{{- if .Values.telemetry.tenantName }}
- --otel-tenant-name={{ .Values.telemetry.tenantName | quote }}
{{- end }}
- --otel-tenant-attr={{ .Values.telemetry.tenantAttr }}
{{- if .Values.telemetry.mcpUrl }}
- --ego-mcp-url={{ .Values.telemetry.mcpUrl }}
{{- end }}
{{- if and .Values.telemetry.tokenSecret.name (not $substrate) }}
- --otel-token-file=/secrets/otel/{{ .Values.telemetry.tokenSecret.key }}
{{- end }}
- --otel-content={{ .Values.telemetry.content }}
{{- end }}
- --log-level={{ .Values.agent.logLevel }}
{{- if .Values.agent.logContent }}
- --log-content
{{- end }}
{{- if .Values.agent.heartbeatId }}
- --heartbeat-id={{ .Values.agent.heartbeatId }}
{{- end }}
{{- if hasKey .Values.agent "maxTasks" }}
- --max-tasks={{ int .Values.agent.maxTasks }}
{{- end }}
{{- end -}}
