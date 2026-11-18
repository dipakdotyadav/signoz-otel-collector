package signozingestionkeyauthextension

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
)

type resolverFunc func(context.Context, string) (tenantIdentity, error)

func (f resolverFunc) Resolve(ctx context.Context, secret string) (tenantIdentity, error) {
	return f(ctx, secret)
}

func TestCredentialFromSources(t *testing.T) {
	secret, err := credentialFromSources(map[string][]string{"Signoz-Ingestion-Key": {"snz_proj_secret"}})
	require.NoError(t, err)
	require.Equal(t, "snz_proj_secret", secret)

	_, err = credentialFromSources(map[string][]string{"signoz-ingestion-key": {"one", "two"}})
	require.Error(t, err)

	_, err = credentialFromSources(map[string][]string{"authorization": {"Bearer token"}})
	require.Error(t, err)
}

func TestAuthenticateAddsTrustedTenantWithoutRetainingSecret(t *testing.T) {
	a := &authenticator{
		resolver: resolverFunc(func(_ context.Context, secret string) (tenantIdentity, error) {
			require.Equal(t, "snz_proj_one-time-secret", secret)
			return tenantIdentity{OrgID: "org-a", ProjectID: "project-a"}, nil
		}),
	}
	ctx, err := a.Authenticate(context.Background(), map[string][]string{"signoz-ingestion-key": {"snz_proj_one-time-secret"}})
	require.NoError(t, err)
	auth := client.FromContext(ctx).Auth
	require.NotNil(t, auth)
	require.Equal(t, "org-a", auth.GetAttribute("org_id"))
	require.Equal(t, "project-a", auth.GetAttribute("project_id"))
	require.Nil(t, auth.GetAttribute("raw"))
	require.Equal(t, []string{"org_id", "project_id"}, auth.GetAttributeNames())
}

func TestAuthenticateFailsClosed(t *testing.T) {
	a := &authenticator{
		resolver: resolverFunc(func(context.Context, string) (tenantIdentity, error) {
			return tenantIdentity{}, errors.New("backend unavailable")
		}),
	}
	_, err := a.Authenticate(context.Background(), map[string][]string{"signoz-ingestion-key": {"bad"}})
	require.ErrorIs(t, err, errInvalidCredential)
}

func TestHTTPResolverUsesPrivateTokenAndSigNozEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "internal-secret", r.Header.Get(internalTokenHeader))
		var req resolveRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "opaque-key", req.IngestionKey)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"orgId":"org-1","projectId":"project-1"}}`))
	}))
	defer server.Close()

	resolver := &httpIdentityResolver{endpoint: server.URL, token: "internal-secret", client: server.Client()}
	got, err := resolver.Resolve(context.Background(), "opaque-key")
	require.NoError(t, err)
	require.Equal(t, tenantIdentity{OrgID: "org-1", ProjectID: "project-1"}, got)
}
