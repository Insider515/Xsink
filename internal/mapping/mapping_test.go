// mapping_test.go — юніт-тести валідації mapping і генерації DDL.
package mapping

import (
	"strings"
	"testing"
)

// validStream — коректний stream для базування тестів.
func validStream() Stream {
	return Stream{
		Source:        "main",
		Name:          "queue:orders",
		ConsumerGroup: "cg",
		Table:         "orders",
		IDColumn:      "redis_stream_id",
		PrimaryKey:    []string{"order_id"},
		OnConflict:    "do_nothing",
		Fields: []Field{
			{Redis: "order_id", Column: "order_id", Type: "text", NotNull: true},
			{Redis: "amount", Column: "amount", Type: "bigint"},
		},
	}
}

// TestValidateOK — коректний mapping проходить валідацію.
func TestValidateOK(t *testing.T) {
	m := &Mapping{Streams: []Stream{validStream()}}
	if err := m.Validate(); err != nil {
		t.Fatalf("valid mapping rejected: %v", err)
	}
}

// TestValidateErrors — кожне порушення структури має давати помилку.
func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(s *Stream)
	}{
		{"name_empty", func(s *Stream) { s.Name = "" }},
		{"group_empty", func(s *Stream) { s.ConsumerGroup = "" }},
		{"table_empty", func(s *Stream) { s.Table = "" }},
		{"table_bad_ident", func(s *Stream) { s.Table = "Orders" }},
		{"fields_empty", func(s *Stream) { s.Fields = nil }},
		{"col_empty", func(s *Stream) { s.Fields[0].Column = "" }},
		{"col_bad_ident", func(s *Stream) { s.Fields[0].Column = "Order-Id" }},
		{"redis_empty", func(s *Stream) { s.Fields[0].Redis = "" }},
		{"type_unsupported", func(s *Stream) { s.Fields[0].Type = "money" }},
		{"on_conflict_bad", func(s *Stream) { s.OnConflict = "replace" }},
		{"idcol_bad_ident", func(s *Stream) { s.IDColumn = "Redis_ID" }},
		{"pk_bad_ident", func(s *Stream) { s.PrimaryKey = []string{"Order"} }},
		{"encoding_bad", func(s *Stream) { s.Fields[0].Type = "bytea"; s.Fields[0].Encoding = "octal" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := validStream()
			c.mutate(&s)
			m := &Mapping{Streams: []Stream{s}}
			if err := m.Validate(); err == nil {
				t.Fatalf("expected validation error for %s", c.name)
			}
		})
	}
}

// TestValidateDuplicates — дублі stream/table/column.
func TestValidateDuplicates(t *testing.T) {
	t.Run("dup_stream_name", func(t *testing.T) {
		a, b := validStream(), validStream()
		b.Table = "orders2"
		m := &Mapping{Streams: []Stream{a, b}}
		if err := m.Validate(); err == nil {
			t.Fatal("expected duplicate stream name error")
		}
	})
	t.Run("dup_column", func(t *testing.T) {
		s := validStream()
		s.Fields[1].Column = "order_id"
		m := &Mapping{Streams: []Stream{s}}
		if err := m.Validate(); err == nil {
			t.Fatal("expected duplicate column error")
		}
	})
}

// TestValidateAgainstSources — прив'язка stream.source до config.redis[].
func TestValidateAgainstSources(t *testing.T) {
	t.Run("single_source_default", func(t *testing.T) {
		s := validStream()
		s.Source = ""
		m := &Mapping{Streams: []Stream{s}}
		if err := m.ValidateAgainstSources(map[string]struct{}{"only": {}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m.Streams[0].Source != "only" {
			t.Fatalf("source not defaulted, got %q", m.Streams[0].Source)
		}
	})
	t.Run("multi_source_missing", func(t *testing.T) {
		s := validStream()
		s.Source = ""
		m := &Mapping{Streams: []Stream{s}}
		err := m.ValidateAgainstSources(map[string]struct{}{"a": {}, "b": {}})
		if err == nil {
			t.Fatal("expected missing-source error for multi-source")
		}
	})
	t.Run("unknown_source", func(t *testing.T) {
		m := &Mapping{Streams: []Stream{validStream()}}
		err := m.ValidateAgainstSources(map[string]struct{}{"other": {}})
		if err == nil {
			t.Fatal("expected unknown-source error")
		}
	})
}

// TestCreateTableSQL — DDL містить очікувані фрагменти.
func TestCreateTableSQL(t *testing.T) {
	s := validStream()
	ddl := s.CreateTableSQL()
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS orders",
		"redis_stream_id TEXT NOT NULL",
		"order_id TEXT NOT NULL",
		"amount BIGINT",
		"ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"PRIMARY KEY (order_id)",
	} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("CreateTableSQL missing %q in:\n%s", want, ddl)
		}
	}
}

// TestAddColumnSQL — ALTER TABLE ADD COLUMN IF NOT EXISTS.
func TestAddColumnSQL(t *testing.T) {
	s := validStream()
	got := s.AddColumnSQL(s.Fields[1])
	want := "ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount BIGINT"
	if got != want {
		t.Fatalf("AddColumnSQL = %q, want %q", got, want)
	}
}

// TestAddColumnSQLNoNotNull — авто-міграція НЕ додає NOT NULL навіть для
// not_null-поля (ALTER ADD COLUMN NOT NULL падає на непорожній таблиці).
func TestAddColumnSQLNoNotNull(t *testing.T) {
	s := validStream()
	got := s.AddColumnSQL(s.Fields[0]) // order_id: not_null=true
	if strings.Contains(got, "NOT NULL") {
		t.Fatalf("AddColumnSQL must not emit NOT NULL, got %q", got)
	}
}

// TestIsValidIdent — межові випадки ідентифікаторів.
func TestIsValidIdent(t *testing.T) {
	ok := []string{"orders", "order_id", "_x", "a1", "x_1_y"}
	bad := []string{"", "Orders", "1x", "order-id", "order id", "таблиця", "a.b"}
	for _, s := range ok {
		if !isValidIdent(s) {
			t.Errorf("isValidIdent(%q) = false, want true", s)
		}
	}
	for _, s := range bad {
		if isValidIdent(s) {
			t.Errorf("isValidIdent(%q) = true, want false", s)
		}
	}
}
