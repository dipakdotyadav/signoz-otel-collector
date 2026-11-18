package signoztenantcontextprocessor

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	orgAttribute     = "signoz.org.id"
	projectAttribute = "signoz.project.id"
)

type tenantContextProcessor struct{ componentID string }

func newProcessor(componentID string) *tenantContextProcessor {
	return &tenantContextProcessor{componentID: componentID}
}

func trustedTenant(ctx context.Context) (string, string, error) {
	auth := client.FromContext(ctx).Auth
	if auth == nil {
		return "", "", errors.New("trusted ingestion identity missing from request context")
	}
	orgID, orgOK := auth.GetAttribute("org_id").(string)
	projectID, projectOK := auth.GetAttribute("project_id").(string)
	if !orgOK || !projectOK || orgID == "" || projectID == "" {
		return "", "", errors.New("trusted ingestion identity is incomplete")
	}
	return orgID, projectID, nil
}

func applyTenant(attrs interface {
	PutStr(string, string)
	Remove(string) bool
}, orgID, projectID string) {
	// Remove client-controlled aliases before writing canonical values.
	attrs.Remove("org_id")
	attrs.Remove("project_id")
	attrs.Remove("organization_id")
	attrs.Remove("signoz-ingestion-key")
	attrs.Remove("SIGNOZ-API-KEY")
	attrs.PutStr(orgAttribute, orgID)
	attrs.PutStr(projectAttribute, projectID)
}

func (p *tenantContextProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	orgID, projectID, err := trustedTenant(ctx)
	if err != nil {
		return td, fmt.Errorf("%s: %w", p.componentID, err)
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		applyTenant(rss.At(i).Resource().Attributes(), orgID, projectID)
	}
	return td, nil
}

func (p *tenantContextProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	orgID, projectID, err := trustedTenant(ctx)
	if err != nil {
		return md, fmt.Errorf("%s: %w", p.componentID, err)
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		applyTenant(rms.At(i).Resource().Attributes(), orgID, projectID)
	}
	return md, nil
}

func (p *tenantContextProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	orgID, projectID, err := trustedTenant(ctx)
	if err != nil {
		return ld, fmt.Errorf("%s: %w", p.componentID, err)
	}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		applyTenant(rls.At(i).Resource().Attributes(), orgID, projectID)
	}
	return ld, nil
}
