package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/tursom/turntf/internal/auth"
)

func newHashCommand(ioCfg commandIO) *cobra.Command {
	var password string
	var readStdin bool
	cmd := &cobra.Command{
		Use:   "hash",
		Short: "Generate a bcrypt password hash",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if password != "" && readStdin {
				return fmt.Errorf("--password and --stdin cannot be used together")
			}
			return runHash(password, readStdin, ioCfg.Stdout, ioCfg.Stdin)
		},
	}
	cmd.Flags().StringVar(&password, "password", "", "password to hash")
	cmd.Flags().BoolVar(&readStdin, "stdin", false, "read password from stdin")
	return cmd
}

func runHash(password string, readStdin bool, stdout io.Writer, stdin io.Reader) error {
	plain, err := resolvePasswordInput(stdout, stdin, password, readStdin)
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
