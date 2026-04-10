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
	"strings"
	"time"

	"notifier/internal/config"
	"notifier/internal/httpapi"
	"notifier/internal/security"
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

	command := args[0]
	switch command {
	case "help", "-h", "--help":
		printUsage(stdout)
		return nil
	case "serve":
		return runServe(args[1:])
	case "apikey":
		return runAPIKey(args[1:])
	default:
		return usageError(command)
	}
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "config.toml", "path to config file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	return serve(cfg)
}

func serve(cfg config.Config) error {
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Init(context.Background()); err != nil {
		return err
	}

	api := httpapi.New(st, cfg.UserSessionTTL)
	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("notification service listening on %s", cfg.Addr)
	log.Printf("sqlite database: %s", cfg.DBPath)
	return server.ListenAndServe()
}

func runAPIKey(args []string) error {
	fs := flag.NewFlagSet("apikey", flag.ContinueOnError)
	configPath := fs.String("config", "config.toml", "path to config file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: notifier apikey <create|list> [...]")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	ctx := context.Background()
	if err := st.Init(ctx); err != nil {
		return err
	}

	switch rest[0] {
	case "create":
		if len(rest) < 2 {
			return fmt.Errorf("usage: notifier apikey [-config config.toml] create <service-name>")
		}
		name := strings.TrimSpace(rest[1])
		if name == "" {
			return fmt.Errorf("service name cannot be empty")
		}
		token, err := security.GenerateOpaqueToken("ntfsk_")
		if err != nil {
			return err
		}
		key, err := st.CreateServiceKey(ctx, name, security.HashToken(token), "ntfsk_")
		if err != nil {
			if err == store.ErrConflict {
				return fmt.Errorf("service name already exists")
			}
			return err
		}
		fmt.Printf("service api key created\n")
		fmt.Printf("name: %s\n", key.Name)
		fmt.Printf("created_at: %s\n", key.CreatedAt.Format(time.RFC3339))
		fmt.Printf("api_key: %s\n", token)
		fmt.Printf("use header: X-API-Key: %s\n", token)
		return nil
	case "list":
		keys, err := st.ListServiceKeys(ctx)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			fmt.Println("no service api keys found")
			return nil
		}
		for _, key := range keys {
			lastUsed := "never"
			if key.LastUsedAt != nil {
				lastUsed = key.LastUsedAt.Format(time.RFC3339)
			}
			fmt.Printf("- name=%s created_at=%s last_used_at=%s\n", key.Name, key.CreatedAt.Format(time.RFC3339), lastUsed)
		}
		return nil
	default:
		return fmt.Errorf("usage: notifier apikey [-config config.toml] <create|list> [...]")
	}
}

func usageError(command string) error {
	return fmt.Errorf("unknown command %q\n\n%s", command, usageText())
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, usageText())
}

func usageText() string {
	return "usage:\n  notifier serve [-config config.toml]\n  notifier apikey [-config config.toml] create <service-name>\n  notifier apikey [-config config.toml] list"
}
