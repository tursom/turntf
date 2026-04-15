package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

type curveConfig struct {
	ServerPublicKey string
	ServerSecretKey string
	ClientPublicKey string
	ClientSecretKey string
}

func newCurveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "curve",
		Short: "Manage ZeroMQ CURVE helpers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "gen",
		Short: "Generate a ZeroMQ CURVE config snippet",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCurveGen(cmd.OutOrStdout())
		},
	})
	return cmd
}

func runCurveGen(stdout io.Writer) error {
	cfg, err := generateCurveConfig()
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(stdout, `[services.zeromq]
security = "curve"

[services.zeromq.curve]
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
