package signozingestionkeyauthextension

import (
	"testing"

	"go.opentelemetry.io/collector/config/configopaque"
)

func TestConfigValidateResolverEndpoint(t *testing.T) {
	tests := []struct {
		name      string
		endpoint  string
		insecure  bool
		wantError bool
	}{
		{name: "https is accepted", endpoint: "https://signoz-api.internal/api/v2/internal/ingestion-keys/resolve"},
		{name: "localhost http is accepted for local development", endpoint: "http://localhost:8080/api/v2/internal/ingestion-keys/resolve", insecure: true},
		{name: "ipv4 loopback http is accepted", endpoint: "http://127.0.0.1:8080/resolve", insecure: true},
		{name: "ipv6 loopback http is accepted", endpoint: "http://[::1]:8080/resolve", insecure: true},
		{name: "http without explicit opt in is rejected", endpoint: "http://localhost:8080/resolve", wantError: true},
		{name: "non loopback http is rejected even with opt in", endpoint: "http://signoz-api.internal:8080/resolve", insecure: true, wantError: true},
		{name: "public ip http is rejected even with opt in", endpoint: "http://192.0.2.10:8080/resolve", insecure: true, wantError: true},
		{name: "relative endpoint is rejected", endpoint: "/api/v2/internal/ingestion-keys/resolve", insecure: true, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				ResolverEndpoint: tt.endpoint,
				InternalToken:    configopaque.String("test-internal-token"),
				AllowInsecure:    tt.insecure,
			}
			err := cfg.Validate()
			if tt.wantError && err == nil {
				t.Fatal("expected validation error")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}
