package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const defaultLogLevel = "info"

type runtimeLoggingConfig struct {
	Level    string
	FilePath string
}

func (c loggingConfig) runtimeConfig() (runtimeLoggingConfig, error) {
	level := strings.ToLower(strings.TrimSpace(c.Level))
	if level == "" {
		level = defaultLogLevel
	}
	if _, err := parseLogLevel(level); err != nil {
		return runtimeLoggingConfig{}, err
	}

	filePath := strings.TrimSpace(c.FilePath)
	if filePath != "" {
		filePath = filepath.Clean(filePath)
	}
	return runtimeLoggingConfig{
		Level:    level,
		FilePath: filePath,
	}, nil
}

func parseLogLevel(level string) (zerolog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return zerolog.NoLevel, fmt.Errorf("logging.level %q is invalid; supported values are debug, info, warn, error", level)
	}
}

func configureDefaultLogger(console io.Writer) {
	closeLogger, err := configureLogger(runtimeLoggingConfig{Level: defaultLogLevel}, console)
	if err == nil {
		_ = closeLogger()
	}
}

func configureLogger(cfg runtimeLoggingConfig, console io.Writer) (func() error, error) {
	levelName := cfg.Level
	if strings.TrimSpace(levelName) == "" {
		levelName = defaultLogLevel
	}
	level, err := parseLogLevel(levelName)
	if err != nil {
		return nil, err
	}
	if console == nil {
		console = io.Discard
	}

	zerolog.TimeFieldFormat = time.RFC3339
	consoleWriter := zerolog.ConsoleWriter{
		Out:        console,
		TimeFormat: time.RFC3339,
		NoColor:    true,
	}

	outputs := []io.Writer{consoleWriter}
	var logFile *os.File
	if cfg.FilePath != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.FilePath), 0o755); err != nil {
			return nil, fmt.Errorf("create log directory %s: %w", filepath.Dir(cfg.FilePath), err)
		}
		logFile, err = os.OpenFile(cfg.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open log file %s: %w", cfg.FilePath, err)
		}
		outputs = append(outputs, logFile)
	}

	writer := zerolog.MultiLevelWriter(outputs...)
	log.Logger = zerolog.New(writer).Level(level).With().Timestamp().Logger()
	return func() error {
		if logFile == nil {
			return nil
		}
		return logFile.Close()
	}, nil
}
