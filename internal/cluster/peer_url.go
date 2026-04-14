package cluster

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

const (
	peerSchemeWebSocket    = "ws"
	peerSchemeWebSocketTLS = "wss"
	peerSchemeZeroMQTCP    = "zmq+tcp"
	zeroMQBindSchemeTCP    = "tcp"
)

func normalizeConfiguredPeerURL(raw string) (string, error) {
	return normalizePeerURLScheme(raw, true)
}

func normalizePeerURL(raw string) (string, error) {
	return normalizePeerURLScheme(raw, true)
}

func normalizeZeroMQBindURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("zeromq bind url cannot be empty")
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse zeromq bind url: %w", err)
	}
	if strings.ToLower(parsed.Scheme) != zeroMQBindSchemeTCP {
		return "", fmt.Errorf("zeromq bind url scheme must be tcp")
	}
	if strings.TrimSpace(parsed.Hostname()) == "" {
		return "", errors.New("zeromq bind url host cannot be empty")
	}
	if strings.TrimSpace(parsed.Port()) == "" {
		return "", errors.New("zeromq bind url port cannot be empty")
	}
	if parsed.Path != "" {
		return "", errors.New("zeromq bind url path is not allowed")
	}
	if parsed.RawQuery != "" {
		return "", errors.New("zeromq bind url query is not allowed")
	}
	if parsed.Fragment != "" {
		return "", errors.New("zeromq bind url fragment is not allowed")
	}

	parsed.Scheme = zeroMQBindSchemeTCP
	parsed.Host = normalizedHostPort(parsed.Hostname(), parsed.Port())
	return parsed.String(), nil
}

func isWebSocketPeerURL(raw string) bool {
	scheme, ok := peerURLScheme(raw)
	return ok && (scheme == peerSchemeWebSocket || scheme == peerSchemeWebSocketTLS)
}

func isZeroMQPeerURL(raw string) bool {
	scheme, ok := peerURLScheme(raw)
	return ok && scheme == peerSchemeZeroMQTCP
}

func peerURLScheme(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", false
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "" {
		return "", false
	}
	return scheme, true
}

func normalizePeerURLScheme(raw string, allowZeroMQ bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("peer url cannot be empty")
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse peer url: %w", err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case peerSchemeWebSocket, peerSchemeWebSocketTLS:
		if strings.TrimSpace(parsed.Host) == "" {
			return "", errors.New("peer url host cannot be empty")
		}
		if parsed.Fragment != "" {
			return "", errors.New("peer url fragment is not allowed")
		}
		parsed.Scheme = scheme
		parsed.Host = strings.ToLower(parsed.Host)
		return parsed.String(), nil
	case peerSchemeZeroMQTCP:
		if !allowZeroMQ {
			return "", fmt.Errorf("peer url scheme must be ws, wss, or zmq+tcp")
		}
		if strings.TrimSpace(parsed.Hostname()) == "" {
			return "", errors.New("peer url host cannot be empty")
		}
		if parsed.Hostname() == "0.0.0.0" || parsed.Hostname() == "::" {
			return "", errors.New("peer url host cannot be a wildcard address")
		}
		if strings.TrimSpace(parsed.Port()) == "" {
			return "", errors.New("peer url port cannot be empty")
		}
		if parsed.Path != "" {
			return "", errors.New("peer url path is not allowed")
		}
		if parsed.RawQuery != "" {
			return "", errors.New("peer url query is not allowed")
		}
		if parsed.Fragment != "" {
			return "", errors.New("peer url fragment is not allowed")
		}
		parsed.Scheme = scheme
		parsed.Host = normalizedHostPort(parsed.Hostname(), parsed.Port())
		return parsed.String(), nil
	default:
		if allowZeroMQ {
			return "", fmt.Errorf("peer url scheme must be ws, wss, or zmq+tcp")
		}
		return "", fmt.Errorf("peer url scheme must be ws or wss")
	}
}

func transportForPeerURL(raw string) string {
	switch {
	case isWebSocketPeerURL(raw):
		return transportWebSocket
	case isZeroMQPeerURL(raw):
		return transportZeroMQ
	default:
		return ""
	}
}

func normalizedHostPort(host, port string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + strings.ToLower(host) + "]"
	} else {
		host = strings.ToLower(host)
	}
	return host + ":" + port
}
