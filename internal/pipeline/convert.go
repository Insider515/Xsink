// convert.go — конверсія Redis stream field (завжди string) у Postgres тип.
package pipeline

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"xsink/internal/mapping"
)

// Convert — приводить raw-значення (string з Redis) до Go-значення для pgx.
// Порожній рядок → nil (SQL NULL), якщо not_null=false.
//
// Тексти помилок НЕ містять самого значення raw (лише ім'я поля й тип), щоб
// сирі дані не потрапляли в логи і в dead_letters.error.
func Convert(f mapping.Field, raw string) (any, error) {
	if raw == "" {
		if f.NotNull {
			return nil, fmt.Errorf("field %s empty (not_null)", f.Redis)
		}
		return nil, nil
	}
	switch f.Type {
	case "text", "uuid":
		return raw, nil
	case "integer":
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("field %s: invalid integer", f.Redis)
		}
		return int32(v), nil
	case "bigint":
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("field %s: invalid bigint", f.Redis)
		}
		return v, nil
	case "real":
		v, err := strconv.ParseFloat(raw, 32)
		if err != nil {
			return nil, fmt.Errorf("field %s: invalid real", f.Redis)
		}
		return float32(v), nil
	case "double":
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("field %s: invalid double", f.Redis)
		}
		return v, nil
	case "boolean":
		v, err := parseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("field %s: invalid boolean", f.Redis)
		}
		return v, nil
	case "jsonb":
		var probe any
		if err := json.Unmarshal([]byte(raw), &probe); err != nil {
			return nil, fmt.Errorf("field %s: invalid json", f.Redis)
		}
		return raw, nil // pgx приймає []byte/string для jsonb напряму
	case "timestamptz":
		if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			return t, nil
		}
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			return t, nil
		}
		if sec, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return time.Unix(sec, 0).UTC(), nil
		}
		return nil, fmt.Errorf("field %s: invalid timestamptz", f.Redis)
	case "bytea":
		switch f.Encoding {
		case "", "hex":
			b, err := hex.DecodeString(raw)
			if err != nil {
				return nil, fmt.Errorf("field %s: invalid hex", f.Redis)
			}
			return b, nil
		case "base64":
			b, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				return nil, fmt.Errorf("field %s: invalid base64", f.Redis)
			}
			return b, nil
		default:
			return nil, fmt.Errorf("field %s unknown encoding %q", f.Redis, f.Encoding)
		}
	}
	return nil, fmt.Errorf("field %s unsupported type %q", f.Redis, f.Type)
}

// parseBool — "true"/"false"/"1"/"0"/"t"/"f" case-insensitive.
func parseBool(s string) (bool, error) {
	switch s {
	case "true", "TRUE", "True", "1", "t", "T":
		return true, nil
	case "false", "FALSE", "False", "0", "f", "F":
		return false, nil
	}
	return false, fmt.Errorf("not boolean: %q", s)
}
