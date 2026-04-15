package main

import (
	"flag"
	"fmt"
	"io"
)

type curveConfig struct {
	ServerPublicKey string
	ServerSecretKey string
	ClientPublicKey string
	ClientSecretKey string
}

func runCurve(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		printCurveUsage(stdout)
		return nil
	}

	switch args[0] {
	case "help", "-h", "--help":
		printCurveUsage(stdout)
		return nil
	case "gen":
		return runCurveGen(args[1:], stdout)
	default:
		return fmt.Errorf("unknown curve command %q\n\n%s", args[0], curveUsageText())
	}
}

func runCurveGen(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("curve gen", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintln(stdout, curveGenUsageText())
	}

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("curve gen does not accept positional arguments")
	}

	cfg, err := generateCurveConfig()
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(stdout, `[cluster.zeromq]
security = "curve"

[cluster.zeromq.curve]
server_public_key = %q
server_secret_key = %q
client_public_key = %q
client_secret_key = %q
allowed_client_public_keys = [%q]
`, cfg.ServerPublicKey, cfg.ServerSecretKey, cfg.ClientPublicKey, cfg.ClientSecretKey, cfg.ClientPublicKey)
	return err
}

func generateCurveConfig() (curveConfig, error) {
	serverPublicKey, serverSecretKey, err := generateCurveKeypair()
	if err != nil {
		return curveConfig{}, err
	}
	clientPublicKey, clientSecretKey, err := generateCurveKeypair()
	if err != nil {
		return curveConfig{}, err
	}
	return curveConfig{
		ServerPublicKey: serverPublicKey,
		ServerSecretKey: serverSecretKey,
		ClientPublicKey: clientPublicKey,
		ClientSecretKey: clientSecretKey,
	}, nil
}

func printCurveUsage(w io.Writer) {
	fmt.Fprintln(w, curveUsageText())
}

func curveUsageText() string {
	return "usage:\n  notifier curve gen\n  notifier curve help"
}

func curveGenUsageText() string {
	return "usage:\n  notifier curve gen"
}
