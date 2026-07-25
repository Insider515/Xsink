// storage.go — Postgres через pgbouncer (simple_protocol у DSN обов'язково
// у transaction pooling mode). Auto-migrate + batch INSERT.
package storage

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"xsink/internal/config"
	"xsink/internal/mapping"
)

// Store — обгортка над pool.
type Store struct {
	Pool *pgxpool.Pool
	Log  *slog.Logger
}

// Open — конструює пул, робить Ping. auto_migrate НЕ виконує — це окремий крок.
func Open(ctx context.Context, cfg config.PostgresConfig, log *slog.Logger) (*Store, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("storage: dsn empty")
	}
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("storage: parse dsn: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		poolCfg.MaxConns = cfg.MaxOpenConns
	}
	timeout := cfg.ConnectTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(pingCtx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("storage: pool: %w", err)
	}
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage: ping: %w", err)
	}
	return &Store{Pool: pool, Log: log}, nil
}

// Close — закриває пул.
func (s *Store) Close() {
	if s != nil && s.Pool != nil {
		s.Pool.Close()
	}
}

// EnsureSchema — auto-migrate: CREATE TABLE IF NOT EXISTS + ALTER TABLE ADD
// COLUMN IF NOT EXISTS для кожного stream'а. Плюс окрема dead_letters.
func (s *Store) EnsureSchema(ctx context.Context, m *mapping.Mapping, deadLetterTable string) error {
	for _, st := range m.Streams {
		if _, err := s.Pool.Exec(ctx, st.CreateTableSQL()); err != nil {
			return fmt.Errorf("storage: create %s: %w", st.Table, err)
		}
		for _, f := range st.Fields {
			if _, err := s.Pool.Exec(ctx, st.AddColumnSQL(f)); err != nil {
				return fmt.Errorf("storage: alter %s add %s: %w", st.Table, f.Column, err)
			}
		}
		s.Log.Info("schema_ready", "table", st.Table, "columns", len(st.Fields))
	}
	return s.ensureDeadLetters(ctx, deadLetterTable)
}

// ensureDeadLetters — таблиця для entries, які не пощастило записати.
func (s *Store) ensureDeadLetters(ctx context.Context, table string) error {
	if table == "" {
		table = "dead_letters"
	}
	_, err := s.Pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		    id            BIGSERIAL PRIMARY KEY,
		    stream        TEXT NOT NULL,
		    redis_id      TEXT NOT NULL,
		    raw_fields    JSONB NOT NULL,
		    error         TEXT NOT NULL,
		    failed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, table))
	if err != nil {
		return fmt.Errorf("storage: dead_letters: %w", err)
	}
	_, _ = s.Pool.Exec(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS idx_%s_stream ON %s(stream)`, table, table))
	_, _ = s.Pool.Exec(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS idx_%s_failed_at ON %s(failed_at DESC)`, table, table))
	return nil
}

// BatchInsert — один INSERT з N рядків.
// Формує SQL `INSERT INTO t (c1,c2) VALUES ($1,$2),($3,$4),... ON CONFLICT ...`.
// rows — усі рядки, у кожному рядку len(row) має дорівнювати len(cols).
//
// Ліміт Postgres: 65535 параметрів на запит → якщо batch × cols > 65535,
// caller має розбити на кілька викликів. Для типового batch 100 × 10 col = 1000
// параметрів — з великим запасом.
func (s *Store) BatchInsert(ctx context.Context, st *mapping.Stream, cols []string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	numCols := len(cols)
	var sb strings.Builder
	sb.Grow(64 + len(cols)*8 + len(rows)*(numCols*8+4))
	sb.WriteString("INSERT INTO ")
	sb.WriteString(st.Table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(") VALUES ")
	args := make([]any, 0, len(rows)*numCols)
	pos := 1
	for i, row := range rows {
		if len(row) != numCols {
			return fmt.Errorf("storage: batch row %d has %d cols, expected %d", i, len(row), numCols)
		}
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')
		for j := range row {
			if j > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('$')
			sb.WriteString(itoa(pos))
			pos++
		}
		sb.WriteByte(')')
		args = append(args, row...)
	}
	switch st.OnConflict {
	case "do_nothing":
		sb.WriteString(" ON CONFLICT DO NOTHING")
	case "update":
		if len(st.PrimaryKey) == 0 {
			return fmt.Errorf("storage: on_conflict=update requires primary_key on %s", st.Table)
		}
		var sets []string
		pkSet := map[string]struct{}{}
		for _, pk := range st.PrimaryKey {
			pkSet[pk] = struct{}{}
		}
		for _, c := range cols {
			if _, isPK := pkSet[c]; isPK {
				continue
			}
			sets = append(sets, c+" = EXCLUDED."+c)
		}
		if len(sets) > 0 {
			sb.WriteString(" ON CONFLICT (")
			sb.WriteString(strings.Join(st.PrimaryKey, ", "))
			sb.WriteString(") DO UPDATE SET ")
			sb.WriteString(strings.Join(sets, ", "))
		} else {
			sb.WriteString(" ON CONFLICT DO NOTHING")
		}
	case "error", "":
		// без ON CONFLICT — SQL-помилка при дублі.
	}
	_, err := s.Pool.Exec(ctx, sb.String(), args...)
	return err
}

// InsertDeadLetter — вставка у dead_letters.
func (s *Store) InsertDeadLetter(ctx context.Context, table, stream, redisID string, rawFields []byte, errMsg string) error {
	_, err := s.Pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (stream, redis_id, raw_fields, error) VALUES ($1, $2, $3, $4)`, table),
		stream, redisID, rawFields, errMsg)
	return err
}

// BatchInsertDeadLetters — bulk-варіант.
func (s *Store) BatchInsertDeadLetters(ctx context.Context, table string, items []DeadLetter) error {
	if len(items) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (stream, redis_id, raw_fields, error) VALUES ")
	args := make([]any, 0, len(items)*4)
	pos := 1
	for i, it := range items {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d)", pos, pos+1, pos+2, pos+3)
		pos += 4
		args = append(args, it.Stream, it.RedisID, it.RawFields, it.Error)
	}
	_, err := s.Pool.Exec(ctx, sb.String(), args...)
	return err
}

// PurgeDeadLetters — видаляє записи dead_letters, старші за olderThan (TTL).
// Повертає кількість видалених рядків.
func (s *Store) PurgeDeadLetters(ctx context.Context, table string, olderThan time.Time) (int64, error) {
	tag, err := s.Pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE failed_at < $1", table), olderThan)
	if err != nil {
		return 0, fmt.Errorf("storage: purge dead_letters: %w", err)
	}
	return tag.RowsAffected(), nil
}

// DeadLetter — DTO для bulk-insert.
type DeadLetter struct {
	Stream    string
	RedisID   string
	RawFields []byte
	Error     string
}

// itoa — швидка конверсія int→string без alloc'ів на дрібних числах.
func itoa(i int) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	if i < 100 {
		return string([]byte{byte('0' + i/10), byte('0' + i%10)})
	}
	// fallback — рідко для batch × cols > 100.
	return fmt.Sprintf("%d", i)
}
