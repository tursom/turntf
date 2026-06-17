package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/tursom/turntf/internal/api"
	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/store"
)

// newServeCommand 创建 "serve" 子命令。
// 通过 --config / -c 标志指定 TOML 配置文件路径，默认为 ./config.toml。
// 命令执行时调用 serveRuntime 启动完整的服务生命周期。
func newServeCommand(ioCfg commandIO) *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the TurnTF service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serveRuntime(cmd.Context(), configPath, ioCfg.Stderr)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", defaultConfigPath, "path to TOML config file")
	return cmd
}

// serveRuntime 是系统总入口函数，初始化并连接所有子系统。
// 启动流程：
//
//  1. 配置加载 —— 解析 TOML 文件，环境变量覆盖，填充默认值
//  2. 日志初始化 —— 配置日志级别、控制台输出、文件输出
//  3. 存储引擎 —— 打开 SQLite/Pebble 数据库，初始化 Schema
//  4. 引导管理员 —— 确保初始管理员用户存在
//  5. 事件日志裁剪 —— 可选的后台定时裁剪任务
//  6. 认证签名器 —— 创建 JWT Token 签名器
//  7. 集群网络 —— 组建 Mesh 网络（WebSocket 对等连接）
//  8. API 服务 —— 创建 gRPC 风格的服务层和 HTTP 传输层
//  9. ZeroMQ 监听 —— 可选的 ZeroMQ 协议监听器
//  10. HTTP 服务器 —— 启动 HTTP API 和集群 WebSocket
//  11. 信号等待 —— 阻塞等待服务错误或关闭信号
func serveRuntime(ctx context.Context, configPath string, logOutput io.Writer) error {
	cfg, err := loadServeRuntimeConfig(configPath)
	if err != nil {
		return err
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	closeLogger, err := configureLogger(cfg.Logging, logOutput)
	if err != nil {
		return err
	}
	defer func() {
		_ = closeLogger()
	}()

	st, err := store.Open(cfg.SQLitePath, cfg.StoreOptions)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Init(ctx); err != nil {
		return err
	}
	cfg.Cluster.NodeID = st.NodeID()
	if err := st.EnsureBootstrapAdmin(ctx, cfg.Auth.BootstrapAdmin); err != nil {
		return err
	}
	if cfg.EventLogPruneEnabled {
		result, err := st.PruneEventLogOnce(runCtx)
		if err != nil {
			return err
		}
		logEventLogPruneResult(result)
		startEventLogPruneLoop(runCtx, st, cfg.EventLogPruneInterval)
	}

	signer, err := auth.NewSigner(cfg.Auth.TokenSecret)
	if err != nil {
		return err
	}

	var manager *cluster.Manager
	if cfg.Cluster.Enabled() {
		manager, err = cluster.NewManager(cfg.Cluster, st)
		if err != nil {
			return err
		}
		defer manager.Close()
		if err := manager.Start(runCtx); err != nil {
			return err
		}
	}

	svc := api.New(st, manager)
	httpAPI := api.NewHTTP(svc, api.HTTPOptions{
		NodeID:   st.NodeID(),
		Signer:   signer,
		TokenTTL: time.Duration(cfg.Auth.TokenTTLMinutes) * time.Minute,
	})
	defer httpAPI.Close()
	if manager != nil {
		manager.SetTransientHandler(httpAPI.ReceiveTransientPacket)
		manager.SetLoggedInUsersProvider(httpAPI.ListLoggedInUsers)
	}
	var zeroMQListener *cluster.ZeroMQMuxListener
	if cfg.Services.ZeroMQ.Enabled && cfg.Services.ZeroMQ.BindURL != "" {
		zeroMQListener = cluster.NewZeroMQMuxListenerWithConfig(cfg.Services.ZeroMQ.BindURL, cfg.Services.ZeroMQ)
		if manager != nil {
			zeroMQListener.SetClusterAccept(manager.AcceptZeroMQConn)
		}
		zeroMQListener.SetClientAccept(func(conn cluster.TransportConn) {
			httpAPI.AcceptZeroMQConn(conn)
		})
		if err := zeroMQListener.Start(runCtx); err != nil {
			return err
		}
		defer zeroMQListener.Close()
		if manager != nil {
			manager.SetZeroMQListenerRunning(true)
		}
		log.Info().
			Str("component", "turntf").
			Str("event", "zeromq_listener_started").
			Str("bind_url", cfg.Services.ZeroMQ.BindURL).
			Msg("zeromq listener started")
	}
	apiServer := &http.Server{
		Addr:              cfg.Services.HTTP.ListenAddr,
		Handler:           serveHandler(httpAPI.Handler(), manager),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	log.Info().Str("component", "turntf").Str("event", "config_loaded").Str("path", cfg.ConfigPath).Msg("config loaded")
	log.Info().Str("component", "turntf").Str("event", "node_identity").Int64("node_id", st.NodeID()).Msg("node identity")
	log.Info().Str("component", "turntf").Str("event", "http_api_listening").Str("addr", cfg.Services.HTTP.ListenAddr).Msg("http api listening")
	log.Info().Str("component", "turntf").Str("event", "store_engine").Str("engine", cfg.StoreOptions.Engine).Msg("store engine")
	log.Info().Str("component", "turntf").Str("event", "sqlite_database").Str("path", cfg.SQLitePath).Msg("sqlite database")
	if cfg.StoreOptions.Engine == store.EnginePebble {
		log.Info().
			Str("component", "turntf").
			Str("event", "pebble_database").
			Str("path", cfg.PebblePath).
			Str("profile", string(cfg.StoreOptions.PebbleProfile)).
			Msg("pebble database")
	}
	if manager != nil {
		log.Info().Str("component", "cluster").Str("event", "websocket_listening").Str("addr", cfg.Services.HTTP.ListenAddr).Str("path", cluster.WebSocketPath).Msg("websocket listening")
	}
	go func() {
		errCh <- apiServer.ListenAndServe()
	}()
	// 阻塞等待 HTTP 服务器返回错误（正常关闭时为 ErrServerClosed，视为成功）
	err = <-errCh
	_ = apiServer.Close()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// startEventLogPruneLoop 启动一个后台协程，按指定间隔定时裁剪事件日志。
// 触发裁剪后若发生错误仅记录警告，不中断循环。
func startEventLogPruneLoop(ctx context.Context, st *store.Store, interval time.Duration) {
	if st == nil || interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := st.PruneEventLogOnce(ctx)
				if err != nil {
					log.Warn().
						Str("component", "turntf").
						Str("event", "event_log_prune_failed").
						Err(err).
						Msg("event log prune failed")
					continue
				}
				logEventLogPruneResult(result)
			}
		}
	}()
}

// logEventLogPruneResult 记录事件日志裁剪结果。
// 当修剪了事件时使用 Info 级别，无事件修剪时使用 Debug 级别。
func logEventLogPruneResult(result store.EventLogPruneResult) {
	event := log.Debug()
	message := "event log prune finished without changes"
	if result.TrimmedEvents > 0 {
		event = log.Info()
		message = "event log prune completed"
	}
	event.
		Str("component", "turntf").
		Str("event", "event_log_pruned").
		Int("max_events_per_origin", result.MaxEventsPerOrigin).
		Int("origins_affected", result.OriginsAffected).
		Int64("trimmed_events", result.TrimmedEvents).
		Msg(message)
}

// serveHandler 创建顶层 HTTP 路由处理器。
// 将 API 处理器挂载到 "/"，如果启用了集群则额外挂载 WebSocket 路径。
func serveHandler(apiHandler http.Handler, manager *cluster.Manager) http.Handler {
	rootMux := http.NewServeMux()
	rootMux.Handle("/", apiHandler)
	if manager != nil {
		rootMux.Handle(cluster.WebSocketPath, manager.Handler())
	}
	return rootMux
}
