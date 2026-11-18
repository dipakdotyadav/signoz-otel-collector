# SigNoz Tenant Context Processor

This processor copies the authenticated organization and project identity from the Collector request context into reserved resource attributes:

- `signoz.org.id`
- `signoz.project.id`

It overwrites those reserved attributes on every resource and removes common client-supplied identity aliases before writing trusted values. It also removes credential-like resource attributes. If the authenticator did not attach a complete identity, processing fails closed.

Enable this processor only in the multi-tenant OTLP pipelines, after the OTLP receiver's authenticator and before any processor, connector, or exporter that derives or persists tenant-scoped data. All traces, metrics, and logs pipelines must include it.
