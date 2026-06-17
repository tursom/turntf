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

// 默认日志级别
const defaultLogLevel = "info"

// runtimeLoggingConfig 是经过验证的运行时日志配置。
// 由 loggingConfig.runtimeConfig() 从 TOML 配置转换而来。
type runtimeLoggingConfig struct {
	// Level 日志级别（debug / info / warn / error）
	Level string
	// FilePath 日志文件路径，为空表示仅控制台输出
	FilePath string
}

// runtimeConfig 将 TOML 日志配置转换为运行时类型。
// 空级别使用默认值 "info"，空文件路径不写文件日志。
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

// parseLogLevel 将日志级别字符串解析为 zerolog.Level 枚举值。
// 支持的级别：debug、info（默认）、warn、error。
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

// configureDefaultLogger 在 main 函数早期调用，初始化使用默认级别的控制台日志记录器。
// 此日志器仅在配置加载前使用，serveRuntime 会调用 configureLogger 重新配置。
func configureDefaultLogger(console io.Writer) {
	closeLogger, err := configureLogger(runtimeLoggingConfig{Level: defaultLogLevel}, console)
	if err == nil {
		_ = closeLogger()
	}
}

// configureLogger 配置全局日志记录器（zerolog）。
// 支持同时输出到控制台和文件：
//   - console：控制台输出（无颜色，RFC3339 时间格式）
//   - cfg.FilePath：可选的日志文件（追加写入）
//
// 返回的闭包用于关闭日志文件，应在服务退出时调用。
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
