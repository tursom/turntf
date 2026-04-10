package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr                string        `json:"addr"`
	DBPath              string        `json:"db_path"`
	UserSessionTTL      time.Duration `json:"-"`
	UserSessionTTLHours int           `json:"user_session_ttl_hours"`
}

func Load(path string) (Config, error) {
	cfg := Config{
		Addr:                ":8080",
		DBPath:              "./data/notifier.db",
		UserSessionTTLHours: 24,
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config file %q: %w", path, err)
	}

	if err := parseTOML(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config file %q: %w", path, err)
	}

	if cfg.Addr == "" {
		return Config{}, fmt.Errorf("config addr cannot be empty")
	}
	if cfg.DBPath == "" {
		return Config{}, fmt.Errorf("config db_path cannot be empty")
	}
	if cfg.UserSessionTTLHours <= 0 {
		return Config{}, fmt.Errorf("config user_session_ttl_hours must be greater than 0")
	}

	cfg.UserSessionTTL = time.Duration(cfg.UserSessionTTLHours) * time.Hour
	return cfg, nil
}

func parseTOML(data []byte, cfg *Config) error {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(stripComment(scanner.Text()))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") {
			return fmt.Errorf("line %d: table syntax is not supported", lineNo)
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("line %d: expected key = value", lineNo)
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		switch key {
		case "addr":
			parsed, err := parseString(value)
			if err != nil {
				return fmt.Errorf("line %d: %w", lineNo, err)
			}
			cfg.Addr = parsed
		case "db_path":
			parsed, err := parseString(value)
			if err != nil {
				return fmt.Errorf("line %d: %w", lineNo, err)
			}
			cfg.DBPath = parsed
		case "user_session_ttl_hours":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("line %d: invalid integer value %q", lineNo, value)
			}
			cfg.UserSessionTTLHours = parsed
		default:
			return fmt.Errorf("line %d: unknown key %q", lineNo, key)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func stripComment(line string) string {
	inQuote := false
	var quote byte

	for i := 0; i < len(line); i++ {
		ch := line[i]
		if inQuote {
			if ch == '\\' && quote == '"' {
				i++
				continue
			}
			if ch == quote {
				inQuote = false
			}
			continue
		}

		if ch == '"' || ch == '\'' {
			inQuote = true
			quote = ch
			continue
		}

		if ch == '#' {
			return line[:i]
		}
	}

	return line
}

func parseString(value string) (string, error) {
	if len(value) < 2 {
		return "", fmt.Errorf("invalid string value %q", value)
	}
	if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
		unquoted, err := strconv.Unquote(value)
		if err == nil {
			return unquoted, nil
		}
		if value[0] == '\'' {
			return value[1 : len(value)-1], nil
		}
		return "", fmt.Errorf("invalid string value %q", value)
	}
	return "", fmt.Errorf("string values must be quoted")
}
