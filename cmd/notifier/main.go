package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"notifier/internal/api"
	"notifier/internal/auth"
	"notifier/internal/cluster"
	"notifier/internal/store"
)

func main() {
	configureDefaultLogger(os.Stderr)
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Fatal().Err(err).Msg("command_failed")
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return runServe(args, stdout)
	}

	switch args[0] {
	case "help", "-h", "--help":
		printUsage(stdout)
		return nil
	case "serve":
		return runServe(args[1:], stdout)
	case "hash":
		return runHash(args[1:], stdout, os.Stdin)
	default:
		if !strings.HasPrefix(args[0], "-") {
			return fmt.Errorf("unknown command %q\n\n%s", args[0], usageText())
		}
		return runServe(args, stdout)
	}
}

func runServe(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stdout)

	configPath := fs.String("config", defaultConfigPath, "path to TOML config file")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("serve does not accept positional arguments")
	}

	cfg, err := loadServeRuntimeConfig(*configPath)
	if err != nil {
		return err
	}
	closeLogger, err := configureLogger(cfg.Logging, os.Stderr)
	if err != nil {
		return err
	}
	defer func() {
		_ = closeLogger()
	}()

	st, err := store.Open(cfg.DBPath, cfg.StoreOptions)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Init(context.Background()); err != nil {
		return err
	}
	cfg.Cluster.NodeID = st.NodeID()
	cfg.Cluster.NodeSlot = st.NodeSlot()
	if err := st.EnsureBootstrapAdmin(context.Background(), cfg.Auth.BootstrapAdmin); err != nil {
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
		manager.Start(context.Background())
	}

	svc := api.New(st, manager)
	httpAPI := api.NewHTTP(svc, api.HTTPOptions{
		NodeID:   st.NodeID(),
		Signer:   signer,
		TokenTTL: time.Duration(cfg.Auth.TokenTTLMinutes) * time.Minute,
	})
	apiServer := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           serveHandler(httpAPI.Handler(), manager, cfg.Cluster.AdvertisePath),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	log.Info().Str("component", "notifier").Str("event", "config_loaded").Str("path", cfg.ConfigPath).Msg("config loaded")
	log.Info().Str("component", "notifier").Str("event", "http_api_listening").Str("addr", cfg.APIAddr).Msg("http api listening")
	log.Info().Str("component", "notifier").Str("event", "sqlite_database").Str("path", cfg.DBPath).Msg("sqlite database")
	if manager != nil {
		log.Info().Str("component", "cluster").Str("event", "websocket_listening").Str("addr", cfg.APIAddr).Str("path", cfg.Cluster.AdvertisePath).Msg("websocket listening")
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

func printUsage(w io.Writer) {
	fmt.Fprintln(w, usageText())
}

func usageText() string {
	return "usage:\n  notifier serve [-config ./config.toml]\n  notifier hash [-password xxx | -stdin]\n  notifier help"
}

func serveHandler(apiHandler http.Handler, manager *cluster.Manager, clusterPath string) http.Handler {
	rootMux := http.NewServeMux()
	rootMux.Handle("/", apiHandler)
	if manager != nil {
		rootMux.Handle(clusterPath, manager.Handler())
	}
	return rootMux
}

func runHash(args []string, stdout io.Writer, stdin io.Reader) error {
	fs := flag.NewFlagSet("hash", flag.ContinueOnError)
	fs.SetOutput(stdout)

	password := fs.String("password", "", "password to hash")
	readStdin := fs.Bool("stdin", false, "read password from stdin")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("hash does not accept positional arguments")
	}

	plain, err := resolvePasswordInput(stdout, stdin, *password, *readStdin)
	if err != nil {
		return err
	}
	hash, err := auth.HashPassword(plain)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, hash)
	return err
}

func resolvePasswordInput(stdout io.Writer, stdin io.Reader, explicit string, readStdin bool) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if readStdin {
		data, err := io.ReadAll(stdin)
		if err != nil {
			return "", fmt.Errorf("read stdin: %w", err)
		}
		return strings.TrimRight(string(data), "\r\n"), nil
	}

	reader := bufio.NewReader(stdin)
	fmt.Fprint(stdout, "Password: ")
	password, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read password: %w", err)
	}
	fmt.Fprint(stdout, "Confirm password: ")
	confirm, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read password confirmation: %w", err)
	}

	password = strings.TrimRight(password, "\r\n")
	confirm = strings.TrimRight(confirm, "\r\n")
	if password != confirm {
		return "", fmt.Errorf("passwords do not match")
	}
	return password, nil
}
