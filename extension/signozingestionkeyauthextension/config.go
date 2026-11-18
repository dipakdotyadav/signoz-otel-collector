package signozingestionkeyauthextension

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	ResolverEndpoint string `mapstructure:"resolver_endpoint"`
	InternalToken    string `mapstructure:"internal_token"`
	AllowInsecure    bool   `mapstructure:"allow_insecure"`
}

func (cfg *Config) Validate() error {
	if cfg == nil || strings.TrimSpace(cfg.ResolverEndpoint) == "" {
		return errors.New("resolver_endpoint is required")
	}
	if strings.TrimSpace(cfg.InternalToken) == "" {
		return errors.New("internal_token is required")
	}
	u, err := url.ParseRequestURI(cfg.ResolverEndpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("resolver_endpoint must be an absolute http(s) URL")
	}
	if u.Scheme != "https" && !cfg.AllowInsecure {
		return errors.New("resolver_endpoint must use https unless allow_insecure is explicitly enabled")
	}
	return nil
}
