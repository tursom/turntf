package cluster

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	peerSchemeWebSocket    = "ws"
	peerSchemeWebSocketTLS = "wss"
	peerSchemeZeroMQTCP    = "zmq+tcp"
	peerSchemeLibP2P       = "libp2p"
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

func isLibP2PPeerURL(raw string) bool {
	scheme, ok := peerURLScheme(raw)
	return ok && scheme == peerSchemeLibP2P
}

func peerURLScheme(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}
	if strings.HasPrefix(trimmed, "/") {
		if _, err := ma.NewMultiaddr(trimmed); err != nil {
			return "", false
		}
		return peerSchemeLibP2P, true
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
	if strings.HasPrefix(trimmed, "/") {
		return normalizeLibP2PPeerAddr(trimmed)
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
	case isLibP2PPeerURL(raw):
		return transportLibP2P
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

func normalizeLibP2PListenAddr(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("libp2p listen addr cannot be empty")
	}
	addr, err := ma.NewMultiaddr(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse libp2p listen addr: %w", err)
	}
	if _, id := peer.SplitAddr(addr); id != "" {
		return "", errors.New("libp2p listen addr must not include /p2p")
	}
	return addr.String(), nil
}

func normalizeLibP2PPeerAddr(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("libp2p peer addr cannot be empty")
	}
	addr, err := ma.NewMultiaddr(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse libp2p peer addr: %w", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return "", errors.New("libp2p peer addr must include /p2p peer id")
	}
	if len(info.Addrs) == 0 {
		return "", errors.New("libp2p peer addr must include a transport address")
	}
	return addr.String(), nil
}

func libP2PPeerIDFromAddr(raw string) string {
	addr, err := ma.NewMultiaddr(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	id, err := peer.IDFromP2PAddr(addr)
	if err != nil {
		return ""
	}
	return id.String()
}
