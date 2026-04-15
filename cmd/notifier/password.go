package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

var (
	isTerminalFile       = isTerminal
	readPasswordLineFile = readPasswordLine
)

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
