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
		return runServe(args[1:])
	case "init-store":
		return runInitStore(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usageText())
	}
}

func runInitStore(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("init-store", flag.ContinueOnError)
	fs.SetOutput(stdout)

	dbPath := fs.String("db", "./data/notifier.db", "path to sqlite db")
	nodeID := fs.String("node-id", "node-a", "stable node id")
	nodeSlot := fs.Uint("node-slot", 1, "numeric node slot used for id generation")

	if err := fs.Parse(args); err != nil {
		return err
	}

	st, err := store.Open(*dbPath, store.Options{
		NodeID:   *nodeID,
		NodeSlot: uint16(*nodeSlot),
	})
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Init(context.Background()); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "initialized store at %s for node %s (slot=%d)\n", *dbPath, *nodeID, *nodeSlot)
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)

	addr := fs.String("addr", ":8080", "http listen address")
	dbPath := fs.String("db", "./data/notifier.db", "path to sqlite db")
	nodeID := fs.String("node-id", "node-a", "stable node id")
	nodeSlot := fs.Uint("node-slot", 1, "numeric node slot used for id generation")
	clusterListenAddr := fs.String("cluster-listen-addr", "", "cluster websocket listen address")
	clusterAdvertiseAddr := fs.String("cluster-advertise-addr", "", "cluster websocket advertise address")
	clusterSecret := fs.String("cluster-secret", "", "shared cluster secret")
	var peers peerFlags
	fs.Var(&peers, "peer", "peer in the form node-id=ws://host:port/internal/cluster/ws")

	if err := fs.Parse(args); err != nil {
		return err
	}

	st, err := store.Open(*dbPath, store.Options{
		NodeID:   *nodeID,
		NodeSlot: uint16(*nodeSlot),
	})
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Init(context.Background()); err != nil {
		return err
	}

	clusterCfg := cluster.Config{
		NodeID:        *nodeID,
		NodeSlot:      uint16(*nodeSlot),
		ListenAddr:    *clusterListenAddr,
		AdvertiseAddr: *clusterAdvertiseAddr,
		ClusterSecret: *clusterSecret,
		Peers:         peers,
	}
	manager, err := cluster.NewManager(clusterCfg, st)
	if err != nil {
		return err
	}
	defer manager.Close()
	manager.Start(context.Background())

	svc := api.New(st, manager)
	httpAPI := api.NewHTTP(svc)
	apiServer := &http.Server{
		Addr:              *addr,
		Handler:           httpAPI.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var clusterServer *http.Server
	errCh := make(chan error, 2)
	if strings.TrimSpace(*clusterListenAddr) != "" {
		clusterServer = &http.Server{
			Addr:              *clusterListenAddr,
			Handler:           manager.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Printf("serving cluster websocket on %s", *clusterListenAddr)
			errCh <- clusterServer.ListenAndServe()
		}()
	}

	log.Printf("serving http api on %s", *addr)
	log.Printf("sqlite database: %s", *dbPath)
	go func() {
		errCh <- apiServer.ListenAndServe()
	}()

	err = <-errCh
	if clusterServer != nil {
		_ = clusterServer.Close()
	}
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
	return "usage:\n  notifier serve [-addr :8080] [-db ./data/notifier.db] [-node-id node-a] [-node-slot 1] [-cluster-listen-addr :9080] [-cluster-advertise-addr ws://127.0.0.1:9080/internal/cluster/ws] [-cluster-secret secret] [-peer node-b=ws://127.0.0.1:9081/internal/cluster/ws]\n  notifier init-store [-db ./data/notifier.db] [-node-id node-a] [-node-slot 1]\n  notifier help"
}

type peerFlags []cluster.Peer

func (p *peerFlags) String() string {
	if len(*p) == 0 {
		return ""
	}
	parts := make([]string, 0, len(*p))
	for _, peer := range *p {
		parts = append(parts, peer.NodeID+"="+peer.URL)
	}
	return strings.Join(parts, ",")
}

func (p *peerFlags) Set(value string) error {
	nodeID, url, ok := strings.Cut(strings.TrimSpace(value), "=")
	if !ok || strings.TrimSpace(nodeID) == "" || strings.TrimSpace(url) == "" {
		return fmt.Errorf("peer must be in the form node-id=ws://host:port/internal/cluster/ws")
	}
	*p = append(*p, cluster.Peer{
		NodeID: strings.TrimSpace(nodeID),
		URL:    strings.TrimSpace(url),
	})
	return nil
}
