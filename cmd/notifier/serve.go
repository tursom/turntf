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

func newServeCommand(ioCfg commandIO) *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the notifier service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serveRuntime(cmd.Context(), configPath, ioCfg.Stderr)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", defaultConfigPath, "path to TOML config file")
	return cmd
}

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
			Str("component", "notifier").
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
	log.Info().Str("component", "notifier").Str("event", "config_loaded").Str("path", cfg.ConfigPath).Msg("config loaded")
	log.Info().Str("component", "notifier").Str("event", "node_identity").Int64("node_id", st.NodeID()).Msg("node identity")
	log.Info().Str("component", "notifier").Str("event", "http_api_listening").Str("addr", cfg.Services.HTTP.ListenAddr).Msg("http api listening")
	log.Info().Str("component", "notifier").Str("event", "store_engine").Str("engine", cfg.StoreOptions.Engine).Msg("store engine")
	log.Info().Str("component", "notifier").Str("event", "sqlite_database").Str("path", cfg.SQLitePath).Msg("sqlite database")
	if cfg.StoreOptions.Engine == store.EnginePebble {
		log.Info().Str("component", "notifier").Str("event", "pebble_database").Str("path", cfg.PebblePath).Msg("pebble database")
	}
	if manager != nil {
		log.Info().Str("component", "cluster").Str("event", "websocket_listening").Str("addr", cfg.Services.HTTP.ListenAddr).Str("path", cluster.WebSocketPath).Msg("websocket listening")
	}
	go func() {
		errCh <- apiServer.ListenAndServe()
	}()

	err = <-errCh
	_ = apiServer.Close()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func serveHandler(apiHandler http.Handler, manager *cluster.Manager) http.Handler {
	rootMux := http.NewServeMux()
	rootMux.Handle("/", apiHandler)
	if manager != nil {
		rootMux.Handle(cluster.WebSocketPath, manager.Handler())
	}
	return rootMux
}
