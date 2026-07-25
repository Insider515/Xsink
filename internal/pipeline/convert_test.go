// convert_test.go — юніт-тести конверсії Redis-string → Go-значення для pgx.
package pipeline

import (
	"bytes"
	"testing"
	"time"

	"xsink/internal/mapping"
)

// f — хелпер: будує mapping.Field для тесту.
func f(typ string, notNull bool, enc string) mapping.Field {
	return mapping.Field{Redis: "x", Column: "x", Type: typ, NotNull: notNull, Encoding: enc}
}

// TestConvertOK — успішні конверсії кожного типу; перевіряє значення.
func TestConvertOK(t *testing.T) {
	ts := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name  string
		field mapping.Field
		raw   string
		want  any
	}{
		{"text", f("text", false, ""), "abc", "abc"},
		{"uuid", f("uuid", false, ""), "11111111-1111-1111-1111-111111111111", "11111111-1111-1111-1111-111111111111"},
		{"integer", f("integer", false, ""), "42", int32(42)},
		{"bigint", f("bigint", false, ""), "9000000000", int64(9000000000)},
		{"real", f("real", false, ""), "1.5", float32(1.5)},
		{"double", f("double", false, ""), "1.5", float64(1.5)},
		{"boolean_true", f("boolean", false, ""), "true", true},
		{"boolean_zero", f("boolean", false, ""), "0", false},
		{"jsonb", f("jsonb", false, ""), `{"a":1}`, `{"a":1}`},
		{"ts_rfc3339", f("timestamptz", false, ""), "2020-01-02T03:04:05Z", ts},
		{"ts_unix", f("timestamptz", false, ""), "1577934245", ts},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Convert(c.field, c.raw)
			if err != nil {
				t.Fatalf("Convert(%q) unexpected error: %v", c.raw, err)
			}
			if got != c.want {
				t.Fatalf("Convert(%q) = %#v, want %#v", c.raw, got, c.want)
			}
		})
	}
}

// TestConvertBytea — hex і base64 повертають однакові байти.
func TestConvertBytea(t *testing.T) {
	want := []byte("hello")
	for _, c := range []struct {
		enc, raw string
	}{
		{"", "68656c6c6f"},     // hex за замовчуванням
		{"hex", "68656c6c6f"},  // явний hex
		{"base64", "aGVsbG8="}, // base64
	} {
		got, err := Convert(f("bytea", false, c.enc), c.raw)
		if err != nil {
			t.Fatalf("enc=%q raw=%q error: %v", c.enc, c.raw, err)
		}
		b, ok := got.([]byte)
		if !ok || !bytes.Equal(b, want) {
			t.Fatalf("enc=%q raw=%q = %#v, want %q", c.enc, c.raw, got, want)
		}
	}
}

// TestConvertNullable — порожній рядок при not_null=false → nil (SQL NULL).
func TestConvertNullable(t *testing.T) {
	got, err := Convert(f("integer", false, ""), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("empty nullable = %#v, want nil", got)
	}
}

// TestConvertErrors — конверсії, які мають повертати помилку.
func TestConvertErrors(t *testing.T) {
	cases := []struct {
		name  string
		field mapping.Field
		raw   string
	}{
		{"empty_not_null", f("text", true, ""), ""},
		{"integer_bad", f("integer", false, ""), "x"},
		{"integer_overflow", f("integer", false, ""), "3000000000"},
		{"bigint_bad", f("bigint", false, ""), "x"},
		{"real_bad", f("real", false, ""), "x"},
		{"double_bad", f("double", false, ""), "x"},
		{"boolean_bad", f("boolean", false, ""), "maybe"},
		{"jsonb_bad", f("jsonb", false, ""), "{"},
		{"ts_bad", f("timestamptz", false, ""), "not-a-time"},
		{"bytea_hex_bad", f("bytea", false, "hex"), "zz"},
		{"bytea_base64_bad", f("bytea", false, "base64"), "!!!"},
		{"bytea_enc_bad", f("bytea", false, "weird"), "00"},
		{"type_unsupported", f("money", false, ""), "1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Convert(c.field, c.raw); err == nil {
				t.Fatalf("Convert(%q) expected error, got nil", c.raw)
			}
		})
	}
}
