# Observability & Telemetry Reference

This document outlines the telemetry architecture (logging, tracing, metrics) in Castor, covering configuration, environment baselines, manual overrides, and integration patterns.

---

## 1. Hybrid Configuration Model

Castor uses a **hybrid configuration model** that combines baseline environment profiles (`APP_ENV`) with granular per-pillar manual overrides.

### Precedence Hierarchy
1. **Explicit Programmatic Configuration**: Values explicitly supplied in `telemetry.Config`.
2. **Granular Environment Variables**: Specific switches like `LOG_LEVEL`, `TRACING_ENABLED`, etc.
3. **Baseline Profile**: Coarse environment set via `APP_ENV`, `ENV`, or `ENVIRONMENT`.
4. **Fallback Default**: The `dev` profile.

---

## 2. Baseline Profiles (`APP_ENV`)

Setting `APP_ENV` establishes default behaviors across all telemetry pillars without requiring multiple environment variables:

| Setting | `dev` (Default) | `stage` | `prod` | `test` | `off` |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Log Level** | `DEBUG` | `INFO` | `INFO` | `ERROR` | `ERROR` (discarded) |
| **Log Format** | Text (`slog.NewTextHandler`) | JSON (`slog.NewJSONHandler`) | JSON (`slog.NewJSONHandler`) | Text | Text (discarded) |
| **Log Stdout** | `true` (ON) | `true` (ON) | `false` (OFF / discarded) | `false` (OFF / discarded) | `false` (OFF / discarded) |
| **Log Source** | `false` | `false` | `false` | `false` | `false` |
| **Tracing Enabled** | `true` | `true` | `true` | `false` | `false` |
| **Trace Stdout Exporter** | `true` (ON) | `true` (ON) | `false` (OFF) | `false` (OFF) | `false` (OFF) |
| **Trace OTLP Exporter** | `false` (OFF) | `true` (ON) | `true` (ON) | `false` (OFF) | `false` (OFF) |
| **Trace Sampler** | `always` (100%) | `always` (100%) | `parent_based` | `never` | `never` |
| **Trace Sample Ratio**| `1.0` | `1.0` | `0.05` (5%) | `0.0` | `0.0` |
| **Metrics Enabled** | `true` | `true` | `true` | `true` | `false` |

---

## 3. Granular Environment Variable Overrides

Any pillar or exporter can be overridden individually without changing the baseline profile:

### Logging
- **`LOG_STDOUT_ENABLED`** / `LOGGER_STDOUT_ENABLED`:
  - Choices: `true`, `false`, `1`, `0`
  - Controls whether logs write to standard output or are discarded (silenced). In `prod`, stdout is disabled by default to silence local terminal output while exporter ingestion runs; can be toggled on if container log forwarding is desired.
- **`LOG_LEVEL`** / `LOGGER_LEVEL`:
  - Choices: `debug`, `info`, `warn`, `error`
  - Example: `LOG_LEVEL=debug` (useful for inspecting production services without switching to dev mode).
- **`LOG_FORMAT`** / `LOGGER_FORMAT`:
  - Choices: `text`, `json`
  - Example: `LOG_FORMAT=json` (useful for local containerized log aggregators).
- **`LOG_ADD_SOURCE`**:
  - Choices: `true`, `false`, `1`, `0`
  - Records calling file and line number in log records (off by default to avoid `runtime.Caller` overhead).

### Distributed Tracing (OpenTelemetry)
- **`TRACING_ENABLED`** / `OTEL_TRACING_ENABLED`:
  - Choices: `true`, `false`, `1`, `0`
  - Master switch for OpenTelemetry tracing.
- **`OTEL_SDK_DISABLED``:
  - Choices: `true`, `false`
  - Standard OpenTelemetry flag. Setting `OTEL_SDK_DISABLED=true` completely disables tracing.
- **`TRACE_STDOUT_ENABLED`** / `TRACE_EXPORTER_STDOUT`:
  - Choices: `true`, `false`, `1`, `0`
  - Controls stdout span formatting and printing via `stdouttrace`.
- **`TRACE_OTLP_ENABLED`** / `TRACE_EXPORTER_OTLP` / `OTEL_EXPORTER_OTLP_ENABLED`:
  - Choices: `true`, `false`, `1`, `0`
  - Controls whether spans are exported via gRPC (`otlptracegrpc`) to the OpenTelemetry Collector.
- **`OTEL_EXPORTER_OTLP_ENDPOINT`** / `OTEL_OTLP_ENDPOINT`:
  - Host/port target of the OpenTelemetry Collector (e.g. `otel-collector:4317` or `localhost:4317`).
- **`OTEL_EXPORTER_OTLP_INSECURE`**:
  - Choices: `true`, `false`, `1`, `0` (defaults to `true`).
- **`TRACE_SAMPLER`** / `OTEL_TRACES_SAMPLER`:
  - Choices: `always`, `never`, `ratio`, `parent_based`
  - Controls the span sampling decision.
- **`TRACE_SAMPLE_RATIO`** / `OTEL_TRACES_SAMPLER_ARG`:
  - Choices: Float from `0.0` to `1.0` (e.g. `0.02` for 2% sampling).

### Metrics (Prometheus)
- **`METRICS_ENABLED`** / `PROMETHEUS_ENABLED`:
  - Choices: `true`, `false`, `1`, `0`
  - Controls whether the Prometheus registry registers active metric collectors and whether `/metrics` returns data. When disabled, `/metrics` returns HTTP 404 and collectors use zero-allocation no-ops.

---

## 4. Service-Specific Instrumentation

### `auth-svc`
- **HTTP Ingress**: `FiberMiddleware` extracts W3C trace context (`traceparent`, `baggage`), begins server spans (`HTTP {METHOD} {ROUTE_PATTERN}`), records status codes and duration histograms, and emits structured access logs.
- **JWT Context**: Attaches `user.id` and `user.role` as attributes to active OpenTelemetry spans.
- **Business Operations**: Spans for `auth.register` (with child `auth.hash_password`), `auth.login` (with child `auth.verify_password`), `auth.create_credential`, `auth.list_credentials`, `auth.revoke_credential`, and `auth.validate_access_key`.
- **Database Layer**: PostgreSQL connection pool instrumented with `otelpgx` (`pgxpool.Config.ConnConfig.Tracer`), producing child spans for executed queries and recording pool statistics.
- **Metrics Endpoint**: Exposed on `GET /metrics`.

---

## 5. OpenTelemetry Collector & Grafana Cloud

For forwarding cluster observability to **Grafana Cloud**, an OpenTelemetry Collector instance is defined in [`deploy/docker-compose/docker-compose.auth.yml`](file:///home/tharun/castor/deploy/docker-compose/docker-compose.auth.yml) backed by [`deploy/docker-compose/otel-collector-config.yaml`](file:///home/tharun/castor/deploy/docker-compose/otel-collector-config.yaml).

### Pipeline Configuration
- **Receivers**:
  - `otlp`: Ingests gRPC (`:4317`) and HTTP (`:4318`) traces, metrics, and logs from cluster services.
  - `prometheus`: Scrapes Prometheus metrics directly from `auth-svc:9095/metrics` every 15s.
- **Processors**: `memory_limiter` and `batch`.
- **Exporters**:
  - `otlphttp/grafana_cloud`: Forwards all traces, metrics, and logs to Grafana Cloud OTLP Gateway using HTTP Basic Authentication (`basicauth/grafana_cloud`).
  - `debug`: Emits basic telemetry ingestion summaries to collector container logs.

### Required Grafana Cloud Environment Variables
In `.env` or system environment:
- `GRAFANA_CLOUD_OTLP_ENDPOINT`: e.g. `https://otlp-gateway-prod-us-east-0.grafana.net/otlp`
- `GRAFANA_CLOUD_INSTANCE_ID`: Grafana Cloud User / Instance ID (numeric)
- `GRAFANA_CLOUD_API_KEY`: Grafana Cloud Access Policy Token (with `traces:write`, `metrics:write`, `logs:write` permissions)
