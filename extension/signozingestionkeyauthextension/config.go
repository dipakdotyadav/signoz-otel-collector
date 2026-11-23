package signozingestionkeyauthextension

import (
	"errors"
	"net"
	"net/url"
	"strings"

	"go.opentelemetry.io/collector/config/configopaque"
)

type Config struct {
	ResolverEndpoint string              `mapstructure:"resolver_endpoint"`
	InternalToken    configopaque.String `mapstructure:"internal_token"`
	AllowInsecure    bool                `mapstructure:"allow_insecure"`
}

func (cfg *Config) Validate() error {
	if cfg == nil || strings.TrimSpace(cfg.ResolverEndpoint) == "" {
		return errors.New("resolver_endpoint is required")
	}
	if strings.TrimSpace(string(cfg.InternalToken)) == "" {
		return errors.New("internal_token is required")
	}
	u, err := url.ParseRequestURI(cfg.ResolverEndpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("resolver_endpoint must be an absolute http(s) URL")
	}
	if u.User != nil || u.Fragment != "" {
		return errors.New("resolver_endpoint must not contain user information or a fragment")
	}
	if u.Scheme == "http" {
		if !cfg.AllowInsecure {
			return errors.New("resolver_endpoint must use https unless allow_insecure is explicitly enabled")
		}
		host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("plain HTTP resolver endpoints are permitted only on loopback")
		}
	}
	return nil
}
