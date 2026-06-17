package main

import (
	"fmt"
	"strings"
)

// z85Alphabet 是 Z85 编码的字母表，包含 85 个可打印 ASCII 字符。
// Z85 是 ZeroMQ 定义的 Base85 编码变体，用于将 CurveZMQ 密钥（32 字节）表示为
// 可打印的文本形式（40 字符）。编码规则：每 4 字节二进制数据编码为 5 个字母表字符。
// 与标准 Base85 不同，Z85 选择了对 URL、源代码等环境友好的字符集，
// 避免了引号和反斜杠等容易引起混淆的字符。
const z85Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"

// encodeZ85 将二进制数据编码为 Z85 字符串。
// data: 待编码的字节切片，长度必须是 4 的倍数。
// 返回值:
//   - string: Z85 编码后的字符串，长度为 len(data)*5/4。
//   - error: 当 data 长度不是 4 的倍数时返回错误。
//
// 编码逻辑: 每 4 字节组成一个 uint32（大端序），然后连续除以 85 取余，
// 用余数作为 z85Alphabet 的索引得到 5 个字符。
func encodeZ85(data []byte) (string, error) {
	if len(data)%4 != 0 {
		return "", fmt.Errorf("z85 data length must be a multiple of 4")
	}

	encoded := make([]byte, len(data)*5/4)
	for src, dst := 0, 0; src < len(data); src, dst = src+4, dst+5 {
		// 将 4 字节以大端序合并为一个 uint32
		value := uint32(data[src])<<24 | uint32(data[src+1])<<16 | uint32(data[src+2])<<8 | uint32(data[src+3])
		// 连续除以 85 取余，得到 5 个 Z85 字符（从高位到低位填充）
		for pos := 4; pos >= 0; pos-- {
			encoded[dst+pos] = z85Alphabet[value%85]
			value /= 85
		}
	}
	return string(encoded), nil
}

// decodeZ85 将 Z85 字符串解码为原始二进制数据。
// text: 待解码的 Z85 字符串，长度必须是 5 的倍数。
// 返回值:
//   - []byte: 解码后的字节切片，长度为 len(text)*4/5。
//   - error: 当文本长度不是 5 的倍数、包含非法字符或值超出 uint32 范围时返回错误。
//
// 解码逻辑: 每 5 个 Z85 字符，通过字母表索引查值，按 85 进制累加得到 uint32，
// 然后拆分为 4 字节（大端序）。
func decodeZ85(text string) ([]byte, error) {
	if len(text)%5 != 0 {
		return nil, fmt.Errorf("z85 text length must be a multiple of 5")
	}

	decoded := make([]byte, len(text)*4/5)
	for src, dst := 0, 0; src < len(text); src, dst = src+5, dst+4 {
		// 将 5 个 Z85 字符从 85 进制转换为 uint64 值
		var value uint64
		for offset := 0; offset < 5; offset++ {
			index := strings.IndexByte(z85Alphabet, text[src+offset])
			if index < 0 {
				return nil, fmt.Errorf("invalid z85 character %q", text[src+offset])
			}
			value = value*85 + uint64(index)
		}
		// 验证值不超过 uint32 范围（Z85 每 5 字符表示 2^32 以内的值）
		if value > uint64(^uint32(0)) {
			return nil, fmt.Errorf("z85 chunk value out of range")
		}

		// 将 uint32 拆分为 4 字节（大端序）
		decoded[dst] = byte(value >> 24)
		decoded[dst+1] = byte(value >> 16)
		decoded[dst+2] = byte(value >> 8)
		decoded[dst+3] = byte(value)
	}
	return decoded, nil
}
