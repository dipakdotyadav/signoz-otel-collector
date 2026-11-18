package signoztenantcontextprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/SigNoz/signoz-otel-collector/processor/signoztenantcontextprocessor/internal/metadata"
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		func() component.Config { return &Config{} },
		processor.WithTraces(createTraces, metadata.Stability),
		processor.WithMetrics(createMetrics, metadata.Stability),
		processor.WithLogs(createLogs, metadata.Stability),
	)
}

type Config struct{}

func createTraces(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	p := newProcessor(set.ID.String())
	return processorhelper.NewTraces(ctx, set, cfg, next, p.processTraces, processorhelper.WithCapabilities(processorCapabilities))
}

func createMetrics(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	p := newProcessor(set.ID.String())
	return processorhelper.NewMetrics(ctx, set, cfg, next, p.processMetrics, processorhelper.WithCapabilities(processorCapabilities))
}

func createLogs(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	p := newProcessor(set.ID.String())
	return processorhelper.NewLogs(ctx, set, cfg, next, p.processLogs, processorhelper.WithCapabilities(processorCapabilities))
}
