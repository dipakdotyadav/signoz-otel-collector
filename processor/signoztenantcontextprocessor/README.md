# SigNoz Tenant Context Processor

This processor copies the authenticated organization and project identity from the Collector request context into reserved resource attributes:

- `signoz.org.id`
- `signoz.project.id`

It overwrites those reserved attributes on every resource and removes common client-supplied identity aliases before writing trusted values. It also removes credential-like resource attributes. If the authenticator did not attach a complete identity, processing fails closed.

Enable this processor only in the multi-tenant OTLP pipelines, after the OTLP receiver's authenticator and before any processor, connector, or exporter that derives or persists tenant-scoped data. All traces, metrics, and logs pipelines must include it.


## Pipeline wiring

The processor is registered in the SigNoz Collector distribution as `signoztenantcontext`.
For a multi-tenant deployment, configure the OTLP receiver with the ingestion-key authenticator and include the tenant processor in **every OTLP signal pipeline**:

```yaml
extensions:
  signozingestionkeyauth:
    resolver_endpoint: https://signoz-api.internal/api/v2/internal/ingestion-keys/resolve
    internal_token: ${env:SIGNOZ_INTERNAL_INGESTION_RESOLVER_TOKEN}
    allow_insecure: false

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
        auth:
          authenticator: signozingestionkeyauth
      http:
        endpoint: 0.0.0.0:4318
        auth:
          authenticator: signozingestionkeyauth

processors:
  signoztenantcontext: {}
  batch: {}

service:
  extensions: [signozingestionkeyauth]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [signoztenantcontext, batch]
      exporters: [clickhousetraces]
    metrics:
      receivers: [otlp]
      processors: [signoztenantcontext, batch]
      exporters: [signozclickhousemetrics]
    logs:
      receivers: [otlp]
      processors: [signoztenantcontext, batch]
      exporters: [clickhouselogsexporter]
```

If traces are transformed into span-derived metrics, place `signoztenantcontext` **before** the span-metrics processor so generated metrics retain the trusted tenant labels. Apply the same ordering to any connector or processor that aggregates, drops, or rewrites resource attributes. Do not expose an unauthenticated OTLP receiver that writes into the same tenant-scoped storage path.

This is a wiring example, not a replacement for the deployment's full exporter, queue, retry, and resource-limit configuration. Keep existing processors and exporters, adding the tenant processor at the earliest safe position in each signal pipeline.


## Community Edition multi-tenant overlay

The repository includes `config/multitenant-config.yaml` as an overlay for the stock default configuration. Load both files, in order, for example:

```sh
./signoz-otel-collector --config=config/default-config.yaml --config=config/multitenant-config.yaml
```

Set `SIGNOZ_INTERNAL_INGESTION_RESOLVER_TOKEN` in the collector environment and replace the resolver URL with the SigNoz API endpoint reachable from the collector. The overlay:

- enables ingestion-key authentication on OTLP gRPC and HTTP;
- removes the unauthenticated Jaeger receiver from the traces pipeline;
- runs `signoztenantcontext` before span-derived metric generation and before batching on all OTLP signal pipelines;
- keeps host metrics in a separate non-OTLP pipeline, so local host telemetry is not rejected for lacking an ingestion-key identity;
- adds `signoz.org.id` and `signoz.project.id` as span-metric dimensions, so generated metric datapoints retain tenant labels.

Do not expose a second unauthenticated OTLP receiver or another receiver writing into the same tenant-scoped storage path. Treat this overlay as the multi-tenant deployment profile; do not apply it to a legacy single-tenant deployment without configuring the resolver and issuing project ingestion keys.
