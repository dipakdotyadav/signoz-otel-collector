# SigNoz Project Ingestion Key Authenticator

This extension authenticates OTLP requests using the `signoz-ingestion-key` HTTP header or gRPC metadata value.

It resolves the opaque credential through the SigNoz API. The resolver must validate the key hash, revocation state, expiry, and project status and return only the trusted `orgId` and `projectId`. The collector never derives tenant identity from client-supplied resource attributes or project headers.

## Configuration

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

service:
  extensions: [signozingestionkeyauth]
```

The resolver call is made for every request; there is intentionally no positive cache in this first implementation, so revocation takes effect on the next authentication attempt. The resolver must be reachable over TLS in production. Resolver failures are fail-closed.

The extension adds only `org_id` and `project_id` to the Collector's authenticated client context. It does not retain the bearer credential in auth attributes or logs.

This is the ingress authentication foundation. A subsequent tenant-context processor must copy the trusted identity into protected resource attributes before the signal enters the exporter pipeline, and SigNoz storage/query paths must enforce those identifiers.
