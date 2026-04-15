package main

import (
	"fmt"
	"strings"
)

const z85Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"

func encodeZ85(data []byte) (string, error) {
	if len(data)%4 != 0 {
		return "", fmt.Errorf("z85 data length must be a multiple of 4")
	}

	encoded := make([]byte, len(data)*5/4)
	for src, dst := 0, 0; src < len(data); src, dst = src+4, dst+5 {
		value := uint32(data[src])<<24 | uint32(data[src+1])<<16 | uint32(data[src+2])<<8 | uint32(data[src+3])
		for pos := 4; pos >= 0; pos-- {
			encoded[dst+pos] = z85Alphabet[value%85]
			value /= 85
		}
	}
	return string(encoded), nil
}

func decodeZ85(text string) ([]byte, error) {
	if len(text)%5 != 0 {
		return nil, fmt.Errorf("z85 text length must be a multiple of 5")
	}

	decoded := make([]byte, len(text)*4/5)
	for src, dst := 0, 0; src < len(text); src, dst = src+5, dst+4 {
		var value uint64
		for offset := 0; offset < 5; offset++ {
			index := strings.IndexByte(z85Alphabet, text[src+offset])
			if index < 0 {
				return nil, fmt.Errorf("invalid z85 character %q", text[src+offset])
			}
			value = value*85 + uint64(index)
		}
		if value > uint64(^uint32(0)) {
			return nil, fmt.Errorf("z85 chunk value out of range")
		}

		decoded[dst] = byte(value >> 24)
		decoded[dst+1] = byte(value >> 16)
		decoded[dst+2] = byte(value >> 8)
		decoded[dst+3] = byte(value)
	}
	return decoded, nil
}
