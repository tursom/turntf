// Package api 提供 turntf 的 HTTP API 层。
// 本文件实现用户元数据（metadata）的类型感知编码/解码机制，支持
// API 用户以显式类型（typed_value）方式提交元数据值，而非仅支持原始字节。
// 支持的类型包括：bytes、bool、string、number、json。
package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/tursom/turntf/internal/store"
)

// metadataTypedValue 表示 API 请求中的类型感知元数据值。
// 客户端通过 Kind 指定数据类型，并填充对应的值字段。
// 编码规则：
//   - bytes: 对 BytesValue 做 base64 解码得到原始字节
//   - bool: 将 BoolValue 序列化为 "true" 或 "false" 字符串
//   - string: 将 StringValue 做 JSON 编码（带引号）
//   - number: 验证 NumberValue 为合法 JSON 数值并标准化
//   - json: 对 JSONValue 做 JSON 压缩（移除空白）
type metadataTypedValue struct {
	// Kind 标明数据类型，取值为 "bytes"、"bool"、"string"、"number"、"json" 之一
	Kind string `json:"kind"`
	// BytesValue 存储 base64 编码的字节数据，仅在 Kind 为 "bytes" 时使用
	BytesValue *string `json:"bytes_value,omitempty"`
	// BoolValue 存储布尔值，仅在 Kind 为 "bool" 时使用
	BoolValue *bool `json:"bool_value,omitempty"`
	// StringValue 存储原始字符串值，仅在 Kind 为 "string" 时使用
	StringValue *string `json:"string_value,omitempty"`
	// NumberValue 存储 JSON 数值，仅在 Kind 为 "number" 时使用
	NumberValue *json.RawMessage `json:"number_value,omitempty"`
	// JSONValue 存储任意 JSON 值，仅在 Kind 为 "json" 时使用
	JSONValue *json.RawMessage `json:"json_value,omitempty"`
}

// metadataRawValueFromRequest 将用户请求中的元数据值转换为原始字节存储格式。
// 请求必须且只能提供 value（原始字节）或 typed_value（类型化值）二者之一，
// 否则返回 ErrInvalidInput。
// 如果提供的是原始字节，直接返回副本；如果是类型化值，委托给 metadataRawValueFromTyped 处理。
func metadataRawValueFromRequest(req userMetadataRequest) ([]byte, error) {
	hasValue := req.Value != nil
	hasTypedValue := req.TypedValue != nil
	if hasValue == hasTypedValue {
		return nil, fmt.Errorf("%w: exactly one of value or typed_value must be provided", store.ErrInvalidInput)
	}
	if hasValue {
		return append([]byte(nil), (*req.Value)...), nil
	}
	return metadataRawValueFromTyped(req.TypedValue)
}

// metadataRawValueFromTyped 将类型化元数据值转换为存储用的原始字节。
// 根据 value.Kind 选择不同的编码路径：
//   - "bytes": 将 base64 编码的字符串解码为原始字节
//   - "bool":  将布尔值编码为 "true" 或 "false" 字符串
//   - "string": 将字符串值做 JSON 编码（结果包含引号）
//   - "number": 验证 JSON 数值格式并标准化
//   - "json":   压缩 JSON（移除不必要的空白）
func metadataRawValueFromTyped(value *metadataTypedValue) ([]byte, error) {
	if value == nil {
		return nil, fmt.Errorf("%w: typed_value cannot be empty", store.ErrInvalidInput)
	}
	switch value.Kind {
	case "bytes":
		if value.BytesValue == nil {
			return nil, fmt.Errorf("%w: typed_value.bytes_value is required", store.ErrInvalidInput)
		}
		decoded, err := base64.StdEncoding.DecodeString(*value.BytesValue)
		if err != nil {
			return nil, fmt.Errorf("%w: typed_value.bytes_value must be base64", store.ErrInvalidInput)
		}
		return decoded, nil
	case "bool":
		if value.BoolValue == nil {
			return nil, fmt.Errorf("%w: typed_value.bool_value is required", store.ErrInvalidInput)
		}
		if *value.BoolValue {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case "string":
		if value.StringValue == nil {
			return nil, fmt.Errorf("%w: typed_value.string_value is required", store.ErrInvalidInput)
		}
		encoded, err := json.Marshal(*value.StringValue)
		if err != nil {
			return nil, fmt.Errorf("%w: typed_value.string_value is invalid", store.ErrInvalidInput)
		}
		return encoded, nil
	case "number":
		if value.NumberValue == nil {
			return nil, fmt.Errorf("%w: typed_value.number_value is required", store.ErrInvalidInput)
		}
		number, err := normalizeMetadataNumberJSON(*value.NumberValue)
		if err != nil {
			return nil, err
		}
		return []byte(number), nil
	case "json":
		if value.JSONValue == nil {
			return nil, fmt.Errorf("%w: typed_value.json_value is required", store.ErrInvalidInput)
		}
		return compactMetadataJSON(*value.JSONValue)
	default:
		return nil, fmt.Errorf("%w: unsupported typed_value.kind %q", store.ErrInvalidInput, value.Kind)
	}
}

// normalizeMetadataNumberJSON 验证并标准化 JSON 数值。
// 使用 json.Decoder 配合 UseNumber() 将输入解析为 json.Number，
// 确保输入是一个合法的 JSON 数值且不包含多余内容。
// 返回标准化后的数字字符串表示。
func normalizeMetadataNumberJSON(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return "", fmt.Errorf("%w: typed_value.number_value cannot be empty", store.ErrInvalidInput)
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var number json.Number
	if err := decoder.Decode(&number); err != nil {
		return "", fmt.Errorf("%w: typed_value.number_value must be a JSON number", store.ErrInvalidInput)
	}
	if err := ensureMetadataJSONEOF(decoder); err != nil {
		return "", err
	}
	return number.String(), nil
}

// compactMetadataJSON 验证并压缩 JSON 值。
// 先校验输入为合法 JSON，然后使用 json.Compact 移除所有不必要的空白字符，
// 返回紧凑格式的 JSON 字节序列。
func compactMetadataJSON(raw json.RawMessage) ([]byte, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("%w: typed_value.json_value cannot be empty", store.ErrInvalidInput)
	}
	if !json.Valid(trimmed) {
		return nil, fmt.Errorf("%w: typed_value.json_value must be valid JSON", store.ErrInvalidInput)
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, trimmed); err != nil {
		return nil, fmt.Errorf("%w: typed_value.json_value must be valid JSON", store.ErrInvalidInput)
	}
	return buf.Bytes(), nil
}

// metadataTypedValueFromRaw 将存储的原始字节反向解析为类型化元数据值。
// 这是 metadataRawValueFromTyped 的逆操作。
// 首先校验原始数据是否为合法的 UTF-8 编码，然后尝试将其解析为 JSON。
// 根据 JSON 值的 Go 类型自动推断 Kind：
//   - bool        → "bool"
//   - string      → "string"
//   - json.Number → "number"
//   - 其余情况（对象、数组等）→ "json"
//
// 非 UTF-8 或非 JSON 数据返回 nil。
func metadataTypedValueFromRaw(raw []byte) *metadataTypedValue {
	if !utf8.Valid(raw) {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil
	}
	if err := ensureMetadataJSONEOF(decoder); err != nil {
		return nil
	}

	switch typed := value.(type) {
	case bool:
		return &metadataTypedValue{
			Kind:      "bool",
			BoolValue: &typed,
		}
	case string:
		return &metadataTypedValue{
			Kind:        "string",
			StringValue: &typed,
		}
	case json.Number:
		number := json.RawMessage(typed.String())
		return &metadataTypedValue{
			Kind:        "number",
			NumberValue: &number,
		}
	default:
		compacted, err := compactMetadataJSON(json.RawMessage(raw))
		if err != nil {
			return nil
		}
		jsonValue := json.RawMessage(compacted)
		return &metadataTypedValue{
			Kind:      "json",
			JSONValue: &jsonValue,
		}
	}
}

// ensureMetadataJSONEOF 确保 JSON Decoder 已完全消费输入流。
// 尝试解码下一个 token，如果未到 EOF 则说明输入包含多个 JSON 值，
// 返回 ErrInvalidInput。用于防止尾随垃圾数据。
func ensureMetadataJSONEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("%w: typed JSON value must contain a single JSON value", store.ErrInvalidInput)
	}
	return nil
}
