package main

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"
)

type commandIO struct {
	Context context.Context
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
}

func run(args []string, stdout io.Writer) error {
	return runWithIO(args, commandIO{
		Context: context.Background(),
		Stdin:   os.Stdin,
		Stdout:  stdout,
		Stderr:  io.Discard,
	})
}

func runWithIO(args []string, ioCfg commandIO) error {
	cmd := newRootCommand(ioCfg)
	cmd.SetArgs(args)
	return cmd.Execute()
}

func newRootCommand(ioCfg commandIO) *cobra.Command {
	ioCfg = normalizeCommandIO(ioCfg)
	cmd := &cobra.Command{
		Use:           "notifier",
		Short:         "Run the TurnTF notifier service and helper commands",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serveRuntime(cmd.Context(), defaultConfigPath, ioCfg.Stderr)
		},
	}
	cmd.SetIn(ioCfg.Stdin)
	cmd.SetOut(ioCfg.Stdout)
	cmd.SetErr(ioCfg.Stderr)
	cmd.SetContext(ioCfg.Context)
	cmd.CompletionOptions.DisableDefaultCmd = true

	cmd.AddCommand(newServeCommand(ioCfg))
	cmd.AddCommand(newHashCommand(ioCfg))
	cmd.AddCommand(newCurveCommand())
	cmd.AddCommand(newCompletionCommand())
	return cmd
}

func normalizeCommandIO(ioCfg commandIO) commandIO {
	if ioCfg.Context == nil {
		ioCfg.Context = context.Background()
	}
	if ioCfg.Stdin == nil {
		ioCfg.Stdin = io.Reader(os.Stdin)
	}
	if ioCfg.Stdout == nil {
		ioCfg.Stdout = io.Discard
	}
	if ioCfg.Stderr == nil {
		ioCfg.Stderr = io.Discard
	}
	return ioCfg
}
