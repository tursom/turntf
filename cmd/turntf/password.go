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

// isTerminalFile 和 readPasswordLineFile 是可模拟的函数变量，用于测试时替换。
var (
	// isTerminalFile 判断文件描述符是否为终端，测试时可替换为模拟函数
	isTerminalFile = isTerminal
	// readPasswordLineFile 从终端读取密码（回显隐藏），测试时可替换为模拟函数
	readPasswordLineFile = readPasswordLine
)

// resolvePasswordInput 从三种来源解析密码明文：
//  1. explicit：--password 标志直接传入的密码字符串
//  2. readStdin：通过 --stdin 标志从标准输入读取
//  3. 交互式提示：在终端上提示用户输入并确认密码（支持回显隐藏）
//
// 返回的密码已去除尾部换行符。
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

// promptHiddenPassword 在终端上提示输入密码，关闭回显以避免密码明文显示在屏幕上。
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

// isTerminal 通过 ioctl 判断给定的文件是否连接到终端。
func isTerminal(file *os.File) bool {
	if file == nil {
		return false
	}
	_, err := unix.IoctlGetTermios(int(file.Fd()), unix.TCGETS)
	return err == nil
}

// readPasswordLine 从终端读取一行密码，读取期间禁用终端回显。
// 函数返回后无论是否出错都会恢复终端原始状态。
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
