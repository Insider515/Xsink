// pipeline_test.go — юніт-тести класифікації помилок, backoff і редакції payload.
package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"xsink/internal/config"
)

// TestIsPermanentDataError — data/constraint помилки = permanent (→ dead_letters),
// решта (мережа, дедлок, недоступність, відсутня схема) = transient (лишити в Redis).
func TestIsPermanentDataError(t *testing.T) {
	perm := []string{"22001", "22P02", "23505", "23502"} // data_exception / integrity
	trans := []string{"08006", "40P01", "40001", "42P01", "42703", "57P01", "53300"}
	for _, c := range perm {
		if !isPermanentDataError(&pgconn.PgError{Code: c}) {
			t.Errorf("code %s: want permanent", c)
		}
	}
	for _, c := range trans {
		if isPermanentDataError(&pgconn.PgError{Code: c}) {
			t.Errorf("code %s: want transient", c)
		}
	}
	if isPermanentDataError(errors.New("plain")) {
		t.Error("plain error must be transient")
	}
	if !isPermanentDataError(fmt.Errorf("wrap: %w", &pgconn.PgError{Code: "23505"})) {
		t.Error("wrapped pg error must be detected")
	}
}

// TestBackoffWithJitter — результат завжди у [0, cap], без паніки на base<=0.
func TestBackoffWithJitter(t *testing.T) {
	base := 500 * time.Millisecond
	for attempt := 1; attempt <= 10; attempt++ {
		for i := 0; i < 100; i++ {
			d := backoffWithJitter(base, attempt)
			if d < 0 || d > 30*time.Second {
				t.Fatalf("attempt %d: backoff %v out of [0,30s]", attempt, d)
			}
		}
	}
	if d := backoffWithJitter(0, 1); d < 0 || d > time.Second {
		t.Fatalf("default base backoff out of range: %v", d)
	}
}

// TestInsertErrTextNoValues — текст помилки для dead_letters не містить значень
// (Detail з реальним значенням не потрапляє), але лишає SQLSTATE.
func TestInsertErrTextNoValues(t *testing.T) {
	pg := &pgconn.PgError{
		Code:    "23505",
		Message: `duplicate key value violates unique constraint "orders_pkey"`,
		Detail:  "Key (order_id)=(SECRET123) already exists.",
	}
	got := insertErrText(pg)
	if strings.Contains(got, "SECRET123") {
		t.Fatalf("insertErrText leaked value: %q", got)
	}
	if !strings.Contains(got, "23505") {
		t.Fatalf("insertErrText missing sqlstate: %q", got)
	}
}

// TestDeadLetterPayloadRedaction — при redact=true у payload нема значень,
// є _redacted+sha256; при redact=false — повний payload зі значенням.
func TestDeadLetterPayloadRedaction(t *testing.T) {
	raw := map[string]string{"card": "4111111111111111"}

	pOff := &Pipeline{Cfg: &config.Config{}}
	full := pOff.deadLetterPayload(raw)
	if !bytes.Contains(full, []byte("4111111111111111")) {
		t.Fatalf("non-redacted payload must contain value: %s", full)
	}

	pOn := &Pipeline{Cfg: &config.Config{}}
	pOn.Cfg.Pipeline.RedactDeadLetterPayload = true
	red := pOn.deadLetterPayload(raw)
	if bytes.Contains(red, []byte("4111111111111111")) {
		t.Fatalf("redacted payload leaked value: %s", red)
	}
	var m map[string]any
	if err := json.Unmarshal(red, &m); err != nil {
		t.Fatalf("redacted payload not valid json: %v", err)
	}
	if m["_redacted"] != true || m["sha256"] == nil {
		t.Fatalf("redacted payload missing markers: %s", red)
	}
}
