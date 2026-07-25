// mapping.go — парсинг і валідація mapping.json + генератор DDL.
//
// Формат:
//   {"streams":[{"name":..., "table":..., "fields":[{"redis":..., "column":..., "type":...}]}]}
package mapping

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// SupportedTypes — SQL-типи, які приймаємо у mapping.
// Значення — конкретний Postgres-тип для DDL.
var SupportedTypes = map[string]string{
	"text":        "TEXT",
	"integer":     "INTEGER",
	"bigint":      "BIGINT",
	"real":        "REAL",
	"double":      "DOUBLE PRECISION",
	"boolean":     "BOOLEAN",
	"jsonb":       "JSONB",
	"timestamptz": "TIMESTAMPTZ",
	"bytea":       "BYTEA",
	"uuid":        "UUID",
}

// Mapping — корінь mapping.json.
type Mapping struct {
	Streams []Stream `json:"streams"`
}

// Stream — конфіг одного stream'а: як читати, куди писати.
type Stream struct {
	Source        string   `json:"source"`         // ім'я Redis-джерела (config.redis[].name); обов'язково якщо >1 source
	Name          string   `json:"name"`           // ім'я Redis stream
	ConsumerGroup string   `json:"consumer_group"` // XGROUP CREATE ...
	BatchSize     int      `json:"batch_size"`     // COUNT у XREADGROUP
	BlockMs       int      `json:"block_ms"`       // BLOCK у XREADGROUP
	Table         string   `json:"table"`          // таблиця в Postgres
	IDColumn      string   `json:"id_column"`      // колонка куди писати Redis stream id (наприклад "1701-0")
	PrimaryKey    []string `json:"primary_key"`    // список колонок PK (для ON CONFLICT)
	OnConflict    string   `json:"on_conflict"`    // do_nothing | update | error
	Fields        []Field  `json:"fields"`         // мапінг redis-field → postgres-column
}

// Field — один field entry → одна колонка.
type Field struct {
	Redis    string `json:"redis"`             // ім'я field у XADD
	Column   string `json:"column"`            // ім'я колонки в Postgres
	Type     string `json:"type"`              // з SupportedTypes
	NotNull  bool   `json:"not_null,omitempty"` // додати NOT NULL у DDL
	Encoding string `json:"encoding,omitempty"` // для bytea: hex | base64
}

// Load — читає і валідує mapping.json.
func Load(path string) (*Mapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("mapping: read %s: %w", path, err)
	}
	var m Mapping
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("mapping: parse: %w", err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

// ValidateAgainstSources — перевіряє що кожен stream.source існує у sources.
// Якщо sources містить рівно 1 елемент — можна опустити stream.source
// (стрім автоматично прив'язується до нього).
func (m *Mapping) ValidateAgainstSources(sources map[string]struct{}) error {
	if len(sources) == 0 {
		return fmt.Errorf("mapping: no redis sources configured")
	}
	var defaultSource string
	if len(sources) == 1 {
		for s := range sources {
			defaultSource = s
		}
	}
	for i := range m.Streams {
		if m.Streams[i].Source == "" {
			if defaultSource == "" {
				return fmt.Errorf("mapping: streams[%d].source is required (multi-source config)", i)
			}
			m.Streams[i].Source = defaultSource
			continue
		}
		if _, ok := sources[m.Streams[i].Source]; !ok {
			return fmt.Errorf("mapping: streams[%d].source %q not found in config.redis[]", i, m.Streams[i].Source)
		}
	}
	return nil
}

// BySource — групує streams за source. Використовується у pipeline.
func (m *Mapping) BySource() map[string][]Stream {
	out := make(map[string][]Stream)
	for _, s := range m.Streams {
		out[s.Source] = append(out[s.Source], s)
	}
	return out
}

// Validate — перевіряє структуру mapping: обов'язкові поля, унікальність, типи.
func (m *Mapping) Validate() error {
	if len(m.Streams) == 0 {
		return fmt.Errorf("mapping: streams empty")
	}
	seenStreams := map[string]struct{}{}
	seenTables := map[string]struct{}{}
	for i, s := range m.Streams {
		prefix := fmt.Sprintf("streams[%d]", i)
		if s.Name == "" {
			return fmt.Errorf("%s.name empty", prefix)
		}
		if _, dup := seenStreams[s.Name]; dup {
			return fmt.Errorf("%s.name %q duplicated", prefix, s.Name)
		}
		seenStreams[s.Name] = struct{}{}
		if s.ConsumerGroup == "" {
			return fmt.Errorf("%s.consumer_group empty", prefix)
		}
		if s.Table == "" {
			return fmt.Errorf("%s.table empty", prefix)
		}
		if _, dup := seenTables[s.Table]; dup {
			return fmt.Errorf("%s.table %q duplicated", prefix, s.Table)
		}
		seenTables[s.Table] = struct{}{}
		if s.BatchSize <= 0 {
			s.BatchSize = 100
			m.Streams[i].BatchSize = 100
		}
		if s.BlockMs <= 0 {
			s.BlockMs = 5000
			m.Streams[i].BlockMs = 5000
		}
		switch s.OnConflict {
		case "", "do_nothing", "update", "error":
			// ок
		default:
			return fmt.Errorf("%s.on_conflict %q must be do_nothing|update|error", prefix, s.OnConflict)
		}
		if s.OnConflict == "" {
			m.Streams[i].OnConflict = "do_nothing"
		}
		if len(s.Fields) == 0 {
			return fmt.Errorf("%s.fields empty", prefix)
		}
		seenCols := map[string]struct{}{}
		seenRedis := map[string]struct{}{}
		for j, f := range s.Fields {
			fp := fmt.Sprintf("%s.fields[%d]", prefix, j)
			if f.Redis == "" {
				return fmt.Errorf("%s.redis empty", fp)
			}
			if f.Column == "" {
				return fmt.Errorf("%s.column empty", fp)
			}
			if !isValidIdent(f.Column) {
				return fmt.Errorf("%s.column %q invalid identifier", fp, f.Column)
			}
			if _, dup := seenCols[f.Column]; dup {
				return fmt.Errorf("%s.column %q duplicated", fp, f.Column)
			}
			seenCols[f.Column] = struct{}{}
			if _, dup := seenRedis[f.Redis]; dup {
				return fmt.Errorf("%s.redis %q duplicated", fp, f.Redis)
			}
			seenRedis[f.Redis] = struct{}{}
			if _, ok := SupportedTypes[f.Type]; !ok {
				return fmt.Errorf("%s.type %q unsupported", fp, f.Type)
			}
			if f.Type == "bytea" && f.Encoding != "" && f.Encoding != "hex" && f.Encoding != "base64" {
				return fmt.Errorf("%s.encoding %q must be hex|base64", fp, f.Encoding)
			}
		}
		if s.IDColumn != "" && !isValidIdent(s.IDColumn) {
			return fmt.Errorf("%s.id_column %q invalid identifier", prefix, s.IDColumn)
		}
		for _, pk := range s.PrimaryKey {
			if !isValidIdent(pk) {
				return fmt.Errorf("%s.primary_key %q invalid identifier", prefix, pk)
			}
		}
		if !isValidIdent(s.Table) {
			return fmt.Errorf("%s.table %q invalid identifier", prefix, s.Table)
		}
	}
	return nil
}

// isValidIdent — [a-z_][a-z0-9_]*. Без quoting'а — безпечно вставляти у SQL.
func isValidIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		case r == '_':
		default:
			return false
		}
	}
	return true
}

// CreateTableSQL — DDL для CREATE TABLE IF NOT EXISTS.
func (s *Stream) CreateTableSQL() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n", s.Table)
	if s.IDColumn != "" {
		fmt.Fprintf(&b, "  %s TEXT NOT NULL,\n", s.IDColumn)
	}
	for _, f := range s.Fields {
		nn := ""
		if f.NotNull {
			nn = " NOT NULL"
		}
		fmt.Fprintf(&b, "  %s %s%s,\n", f.Column, SupportedTypes[f.Type], nn)
	}
	fmt.Fprintln(&b, "  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
	if len(s.PrimaryKey) > 0 {
		fmt.Fprintf(&b, "  , PRIMARY KEY (%s)\n", strings.Join(s.PrimaryKey, ", "))
	}
	b.WriteString(")")
	return b.String()
}

// AddColumnSQL — DDL для ALTER TABLE ADD COLUMN IF NOT EXISTS.
//
// NOT NULL свідомо НЕ додається: `ALTER TABLE ADD COLUMN ... NOT NULL` без
// DEFAULT падає на непорожній таблиці й кладе сервіс. Нова колонка додається
// nullable; якщо потрібен NOT NULL — це робиться окремою контрольованою
// міграцією вручну (backfill + SET NOT NULL). NOT NULL з mapping діє лише при
// первинному CREATE TABLE (порожня таблиця), див. CreateTableSQL.
func (s *Stream) AddColumnSQL(f Field) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
		s.Table, f.Column, SupportedTypes[f.Type])
}
