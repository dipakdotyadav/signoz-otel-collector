package signoztenantcontextprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type tenantAuth struct{ orgID, projectID string }

func (a tenantAuth) GetAttribute(name string) any {
	switch name {
	case "org_id":
		return a.orgID
	case "project_id":
		return a.projectID
	default:
		return nil
	}
}

func (tenantAuth) GetAttributeNames() []string { return []string{"org_id", "project_id"} }

func TestProcessTracesOverwritesClientTenantAttributes(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr(orgAttribute, "forged-org")
	rs.Resource().Attributes().PutStr(projectAttribute, "forged-project")
	rs.Resource().Attributes().PutStr("org_id", "forged-alias")
	rs.Resource().Attributes().PutStr("signoz-ingestion-key", "client-secret")

	ctx := client.NewContext(context.Background(), client.Info{Auth: tenantAuth{orgID: "org-trusted", projectID: "project-trusted"}})
	got, err := newProcessor("signoztenantcontext").processTraces(ctx, td)
	require.NoError(t, err)

	attrs := got.ResourceSpans().At(0).Resource().Attributes()
	org, ok := attrs.Get(orgAttribute)
	require.True(t, ok)
	require.Equal(t, "org-trusted", org.Str())
	project, ok := attrs.Get(projectAttribute)
	require.True(t, ok)
	require.Equal(t, "project-trusted", project.Str())
	_, ok = attrs.Get("org_id")
	require.False(t, ok)
	_, ok = attrs.Get("signoz-ingestion-key")
	require.False(t, ok)
}

func TestTrustedTenantFailsClosed(t *testing.T) {
	_, _, err := trustedTenant(context.Background())
	require.Error(t, err)
}

func TestApplyTenant(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("project_id", "spoofed")
	applyTenant(attrs, "org-1", "project-1")
	org, _ := attrs.Get(orgAttribute)
	project, _ := attrs.Get(projectAttribute)
	require.Equal(t, "org-1", org.Str())
	require.Equal(t, "project-1", project.Str())
}
