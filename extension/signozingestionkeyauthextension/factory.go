package signozingestionkeyauthextension

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"

	"github.com/SigNoz/signoz-otel-collector/extension/signozingestionkeyauthextension/internal/metadata"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.Stability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	config := cfg.(*Config)
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return newAuthenticator(*config, set.TelemetrySettings.Logger), nil
}

const defaultResolverTimeout = 2 * time.Second
