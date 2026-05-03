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

type metadataTypedValue struct {
	Kind        string           `json:"kind"`
	BytesValue  *string          `json:"bytes_value,omitempty"`
	BoolValue   *bool            `json:"bool_value,omitempty"`
	StringValue *string          `json:"string_value,omitempty"`
	NumberValue *json.RawMessage `json:"number_value,omitempty"`
	JSONValue   *json.RawMessage `json:"json_value,omitempty"`
}

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

func ensureMetadataJSONEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("%w: typed JSON value must contain a single JSON value", store.ErrInvalidInput)
	}
	return nil
}
