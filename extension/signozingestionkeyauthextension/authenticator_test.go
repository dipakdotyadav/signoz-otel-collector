package signozingestionkeyauthextension

import (
	"context"
	"errors"
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
