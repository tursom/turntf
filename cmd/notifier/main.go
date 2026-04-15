package main

import (
	"context"
	"os"

	"github.com/rs/zerolog/log"
)

func main() {
	configureDefaultLogger(os.Stderr)
	cmd := newRootCommand(commandIO{
		Context: context.Background(),
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	})
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("command_failed")
	}
}
