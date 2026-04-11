package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"notifier/internal/api"
	"notifier/internal/cluster"
	"notifier/internal/store"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Fatal(err)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		printUsage(stdout)
		return nil
	}

	switch args[0] {
	case "help", "-h", "--help":
		printUsage(stdout)
		return nil
	case "serve":
		return runServe(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usageText())
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

	st, err := store.Open(cfg.DBPath, cfg.StoreOptions)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Init(context.Background()); err != nil {
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
	httpAPI := api.NewHTTP(svc)
	apiServer := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           serveHandler(httpAPI.Handler(), manager, cfg.Cluster.AdvertisePath),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	log.Printf("loaded config from %s", cfg.ConfigPath)
	log.Printf("serving http api on %s", cfg.APIAddr)
	log.Printf("sqlite database: %s", cfg.DBPath)
	if manager != nil {
		log.Printf("serving cluster websocket on %s%s via api listener", cfg.APIAddr, cfg.Cluster.AdvertisePath)
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
	return "usage:\n  notifier serve [-config ./config.toml]\n  notifier help"
}

func serveHandler(apiHandler http.Handler, manager *cluster.Manager, clusterPath string) http.Handler {
	rootMux := http.NewServeMux()
	rootMux.Handle("/", apiHandler)
	if manager != nil {
		rootMux.Handle(clusterPath, manager.Handler())
	}
	return rootMux
}
