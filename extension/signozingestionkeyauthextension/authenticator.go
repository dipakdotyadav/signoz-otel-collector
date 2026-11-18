package signozingestionkeyauthextension

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"go.uber.org/zap"
)

const (
	ingestionKeyHeader  = "signoz-ingestion-key"
	internalTokenHeader = "X-SigNoz-Internal-Token"
)

var (
	errMissingCredential = errors.New("missing signoz-ingestion-key")
	errInvalidCredential = errors.New("invalid project ingestion credential")
)

type tenantIdentity struct {
	OrgID     string
	ProjectID string
}

type identityResolver interface {
	Resolve(context.Context, string) (tenantIdentity, error)
}

type authenticator struct {
	component.StartFunc
	component.ShutdownFunc
	resolver identityResolver
	logger   *zap.Logger
}

var (
	_ extension.Extension  = (*authenticator)(nil)
	_ extensionauth.Server = (*authenticator)(nil)
)

func newAuthenticator(cfg Config, logger *zap.Logger) *authenticator {
	return &authenticator{
		resolver: &httpIdentityResolver{
			endpoint: cfg.ResolverEndpoint,
			token:    cfg.InternalToken,
			client: &http.Client{
				Timeout: defaultResolverTimeout,
				CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
			},
		},
		logger: logger,
	}
}

func (a *authenticator) Authenticate(ctx context.Context, sources map[string][]string) (context.Context, error) {
	secret, err := credentialFromSources(sources)
	if err != nil {
		return ctx, err
	}
	identity, err := a.resolver.Resolve(ctx, secret)
	if err != nil {
		if a.logger != nil {
			a.logger.Warn("project ingestion authentication failed")
		}
		return ctx, errInvalidCredential
	}
	if identity.OrgID == "" || identity.ProjectID == "" {
		return ctx, errInvalidCredential
	}
	info := client.FromContext(ctx)
	info.Auth = &projectAuthData{orgID: identity.OrgID, projectID: identity.ProjectID}
	return client.NewContext(ctx, info), nil
}

func credentialFromSources(sources map[string][]string) (string, error) {
	var values []string
	for key, candidates := range sources {
		if strings.EqualFold(key, ingestionKeyHeader) {
			values = append(values, candidates...)
		}
	}
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
		return "", errMissingCredential
	}
	return strings.TrimSpace(values[0]), nil
}

type projectAuthData struct {
	orgID     string
	projectID string
}

func (a *projectAuthData) GetAttribute(name string) any {
	switch name {
	case "org_id":
		return a.orgID
	case "project_id":
		return a.projectID
	default:
		return nil
	}
}

func (*projectAuthData) GetAttributeNames() []string {
	return []string{"org_id", "project_id"}
}

type httpIdentityResolver struct {
	endpoint string
	token    string
	client   *http.Client
}

type resolveRequest struct {
	IngestionKey string `json:"ingestionKey"`
}

type resolveResponse struct {
	OrgID     string `json:"orgId"`
	ProjectID string `json:"projectId"`
}

func (r *httpIdentityResolver) Resolve(ctx context.Context, secret string) (tenantIdentity, error) {
	body, err := json.Marshal(resolveRequest{IngestionKey: secret})
	if err != nil {
		return tenantIdentity{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return tenantIdentity{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(internalTokenHeader, r.token)
	resp, err := r.client.Do(req)
	if err != nil {
		return tenantIdentity{}, fmt.Errorf("resolve project ingestion key: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return tenantIdentity{}, errInvalidCredential
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 4096))
	var result resolveResponse
	if err := decoder.Decode(&result); err != nil {
		return tenantIdentity{}, err
	}
	if strings.TrimSpace(result.OrgID) == "" || strings.TrimSpace(result.ProjectID) == "" {
		return tenantIdentity{}, errInvalidCredential
	}
	return tenantIdentity{OrgID: result.OrgID, ProjectID: result.ProjectID}, nil
}
