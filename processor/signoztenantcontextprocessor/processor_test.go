package signoztenantcontextprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
	attrs := rs.Resource().Attributes()
	attrs.PutStr(orgAttribute, "forged-org")
	attrs.PutStr(projectAttribute, "forged-project")
	attrs.PutStr("org_id", "forged-org-alias")
	attrs.PutStr("project_id", "forged-project-alias")
	attrs.PutStr("organization_id", "forged-organization-alias")
	attrs.PutStr("signoz-ingestion-key", "client-secret")
	attrs.PutStr("SIGNOZ-API-KEY", "client-api-key")

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
	for _, untrustedKey := range []string{
		"org_id",
		"project_id",
		"organization_id",
		"signoz-ingestion-key",
		"SIGNOZ-API-KEY",
	} {
		_, ok = attrs.Get(untrustedKey)
		require.False(t, ok, "untrusted tenant alias %q must be removed", untrustedKey)
	}
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


func TestProcessMetricsAndLogsApplyTrustedTenant(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{Auth: tenantAuth{orgID: "org-trusted", projectID: "project-trusted"}})
	processor := newProcessor("signoztenantcontext")

	metrics := pmetric.NewMetrics()
	metricResource := metrics.ResourceMetrics().AppendEmpty().Resource()
	metricResource.Attributes().PutStr(projectAttribute, "forged")
	gotMetrics, err := processor.processMetrics(ctx, metrics)
	require.NoError(t, err)
	metricProject, ok := gotMetrics.ResourceMetrics().At(0).Resource().Attributes().Get(projectAttribute)
	require.True(t, ok)
	require.Equal(t, "project-trusted", metricProject.Str())

	logs := plog.NewLogs()
	logResource := logs.ResourceLogs().AppendEmpty().Resource()
	logAttrs := logResource.Attributes()
	logAttrs.PutStr(orgAttribute, "forged-org")
	logAttrs.PutStr(projectAttribute, "forged-project")
	logAttrs.PutStr("org_id", "forged-org-alias")
	logAttrs.PutStr("project_id", "forged-project-alias")
	logAttrs.PutStr("organization_id", "forged-organization-alias")
	logAttrs.PutStr("signoz-ingestion-key", "client-secret")
	logAttrs.PutStr("SIGNOZ-API-KEY", "client-api-key")

	gotLogs, err := processor.processLogs(ctx, logs)
	require.NoError(t, err)
	logAttrs = gotLogs.ResourceLogs().At(0).Resource().Attributes()
	logOrg, ok := logAttrs.Get(orgAttribute)
	require.True(t, ok)
	require.Equal(t, "org-trusted", logOrg.Str())
	logProject, ok := logAttrs.Get(projectAttribute)
	require.True(t, ok)
	require.Equal(t, "project-trusted", logProject.Str())
	for _, untrustedKey := range []string{
		"org_id",
		"project_id",
		"organization_id",
		"signoz-ingestion-key",
		"SIGNOZ-API-KEY",
	} {
		_, ok = logAttrs.Get(untrustedKey)
		require.False(t, ok, "untrusted tenant alias %q must be removed from logs", untrustedKey)
	}
}
