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
	"golang.org/x/sys/unix"

	"github.com/tursom/turntf/internal/api"
	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/cluster"
	"github.com/tursom/turntf/internal/store"
)

var (
	isTerminalFile       = isTerminal
	readPasswordLineFile = readPasswordLine
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
	case "curve":
		return runCurve(args[1:], stdout)
	case "completion":
		return runCompletion(args[1:], stdout)
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
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	closeLogger, err := configureLogger(cfg.Logging, os.Stderr)
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

	if err := st.Init(context.Background()); err != nil {
		return err
	}
	cfg.Cluster.NodeID = st.NodeID()
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
	if manager != nil && cfg.Cluster.ZeroMQ.Enabled && strings.TrimSpace(cfg.Cluster.ZeroMQ.BindURL) != "" {
		zeroMQListener = cluster.NewZeroMQMuxListenerWithConfig(cfg.Cluster.ZeroMQ.BindURL, cfg.Cluster.ZeroMQ)
		zeroMQListener.SetClusterAccept(manager.AcceptZeroMQConn)
		zeroMQListener.SetClientAccept(func(conn cluster.TransportConn) {
			httpAPI.AcceptZeroMQConn(conn)
		})
		if err := zeroMQListener.Start(runCtx); err != nil {
			return err
		}
		defer zeroMQListener.Close()
		manager.SetZeroMQListenerRunning(true)
		log.Info().
			Str("component", "notifier").
			Str("event", "zeromq_listener_started").
			Str("bind_url", cfg.Cluster.ZeroMQ.BindURL).
			Msg("zeromq listener started")
	}
	apiServer := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           serveHandler(httpAPI.Handler(), manager, cfg.Cluster.AdvertisePath),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	log.Info().Str("component", "notifier").Str("event", "config_loaded").Str("path", cfg.ConfigPath).Msg("config loaded")
	log.Info().Str("component", "notifier").Str("event", "node_identity").Int64("node_id", st.NodeID()).Msg("node identity")
	log.Info().Str("component", "notifier").Str("event", "http_api_listening").Str("addr", cfg.APIAddr).Msg("http api listening")
	log.Info().Str("component", "notifier").Str("event", "store_engine").Str("engine", cfg.StoreOptions.Engine).Msg("store engine")
	log.Info().Str("component", "notifier").Str("event", "sqlite_database").Str("path", cfg.SQLitePath).Msg("sqlite database")
	if cfg.StoreOptions.Engine == store.EnginePebble {
		log.Info().Str("component", "notifier").Str("event", "pebble_database").Str("path", cfg.PebblePath).Msg("pebble database")
	}
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
	return "usage:\n  notifier serve [-config ./config.toml]\n  notifier hash [-password xxx | -stdin]\n  notifier curve gen\n  notifier completion zsh\n  notifier help"
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
	if file, ok := stdin.(*os.File); ok && isTerminalFile(file) {
		password, err := promptHiddenPassword(stdout, file, "Password: ")
		if err != nil {
			return "", fmt.Errorf("read password: %w", err)
		}
		confirm, err := promptHiddenPassword(stdout, file, "Confirm password: ")
		if err != nil {
			return "", fmt.Errorf("read password confirmation: %w", err)
		}
		if password != confirm {
			return "", fmt.Errorf("passwords do not match")
		}
		return password, nil
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

func promptHiddenPassword(stdout io.Writer, stdin *os.File, prompt string) (string, error) {
	if _, err := fmt.Fprint(stdout, prompt); err != nil {
		return "", err
	}
	password, err := readPasswordLineFile(stdin)
	if _, printErr := fmt.Fprintln(stdout); printErr != nil && err == nil {
		err = printErr
	}
	if err != nil {
		return "", err
	}
	return strings.TrimRight(password, "\r\n"), nil
}

func isTerminal(file *os.File) bool {
	if file == nil {
		return false
	}
	_, err := unix.IoctlGetTermios(int(file.Fd()), unix.TCGETS)
	return err == nil
}

func readPasswordLine(file *os.File) (string, error) {
	fd := int(file.Fd())
	state, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return "", err
	}
	hidden := *state
	hidden.Lflag &^= unix.ECHO
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &hidden); err != nil {
		return "", err
	}
	defer func() {
		_ = unix.IoctlSetTermios(fd, unix.TCSETS, state)
	}()

	password, err := bufio.NewReader(file).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return password, nil
}
