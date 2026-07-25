// pipeline.go — головна петля: XREADGROUP → batch INSERT → XACK+XDEL pipeline.
//
// Multi-source: кожне Redis-джерело з config.RedisConfig.Name має власного
// клієнта, власний rate-limiter (з redis.PollRatePerSecond того джерела)
// і власний circuit breaker. Один Postgres-store — спільний.
//
// Продуктивність:
//   - INSERT — один multi-row `INSERT ... VALUES (...),(...)... ON CONFLICT`.
//   - XACK+XDEL — один Redis-pipeline на весь batch.
//   - При частковому збої batch — binary split (O(log₂ n) розбиттів на
//     знаходження проблемного entry замість O(n)).
//
// Стабільність:
//   - Circuit breaker per-source (Redis) і глобальний (Postgres).
//   - Помилки конверсії/INSERT → dead_letters bulk; XACK+XDEL все одно
//     виконуються щоб не крутити повторно.
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"
	"golang.org/x/time/rate"

	"xsink/internal/breaker"
	"xsink/internal/config"
	"xsink/internal/mapping"
	"xsink/internal/metrics"
	"xsink/internal/storage"
)

// Pipeline — залежності і стан.
type Pipeline struct {
	Cfg     *config.Config
	Mapping *mapping.Mapping
	Clients map[string]*redis.Client
	Store   *storage.Store
	Log     *slog.Logger

	// perSource — per-source rate limiter + circuit breaker.
	perSource map[string]*sourceState
	// pgCB — глобальний CB для Postgres.
	pgCB *gobreaker.CircuitBreaker[struct{}]
}

// sourceState — власне те що специфічне для одного Redis-джерела.
type sourceState struct {
	limiter *rate.Limiter
	cb      *gobreaker.CircuitBreaker[[]redis.XStream]
}

// New — конструктор.
func New(cfg *config.Config, m *mapping.Mapping, clients map[string]*redis.Client, s *storage.Store, log *slog.Logger) *Pipeline {
	perSource := make(map[string]*sourceState, len(cfg.Redis))
	for _, src := range cfg.Redis {
		rps := src.PollRatePerSecond
		if rps <= 0 {
			rps = 500
		}
		perSource[src.Name] = &sourceState{
			limiter: rate.NewLimiter(rate.Limit(rps), rps),
			cb:      breaker.New[[]redis.XStream]("redis:"+src.Name, cfg.Breaker.MaxConsecutiveFailures, cfg.Breaker.OpenTimeout),
		}
	}
	return &Pipeline{
		Cfg: cfg, Mapping: m, Clients: clients, Store: s, Log: log,
		perSource: perSource,
		pgCB:      breaker.New[struct{}]("postgres", cfg.Breaker.MaxConsecutiveFailures, cfg.Breaker.OpenTimeout),
	}
}

// EnsureGroups — XGROUP CREATE ... MKSTREAM (ідемпотентно) для кожного stream
// у контексті його source-клієнта.
func (p *Pipeline) EnsureGroups(ctx context.Context) error {
	for _, s := range p.Mapping.Streams {
		cli, ok := p.Clients[s.Source]
		if !ok {
			return fmt.Errorf("pipeline: no redis client for source %q", s.Source)
		}
		err := cli.XGroupCreateMkStream(ctx, s.Name, s.ConsumerGroup, "$").Err()
		if err != nil && !isBusyGroup(err) {
			return fmt.Errorf("pipeline: xgroup %s/%s @ %s: %w", s.Name, s.ConsumerGroup, s.Source, err)
		}
		p.Log.Info("group_ready", "source", s.Source, "stream", s.Name, "group", s.ConsumerGroup)
	}
	return nil
}

// isBusyGroup — Redis повертає "BUSYGROUP Consumer Group name already exists".
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Run — блокуючий: піднімає N worker'ів на кожен stream, повертає коли
// ctx скасовано і всі worker'и завершились (drain-and-exit).
func (p *Pipeline) Run(ctx context.Context) error {
	if err := p.EnsureGroups(ctx); err != nil {
		return err
	}
	hostname, _ := os.Hostname()
	n := p.Cfg.Pipeline.Workers
	if n <= 0 {
		n = 1
	}
	var wg sync.WaitGroup
	for _, s := range p.Mapping.Streams {
		for i := 0; i < n; i++ {
			consumerID := fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), i)
			wg.Add(1)
			go func(stream mapping.Stream, cid string) {
				defer wg.Done()
				p.workerLoop(ctx, stream, cid)
			}(s, consumerID)
		}
	}
	wg.Wait()
	return nil
}

// workerLoop — одна горутина: continuous XREADGROUP + batch-обробка.
func (p *Pipeline) workerLoop(ctx context.Context, s mapping.Stream, consumerID string) {
	p.Log.Info("worker_start", "source", s.Source, "stream", s.Name, "consumer", consumerID)
	cli := p.Clients[s.Source]
	st := p.perSource[s.Source]
	for {
		if err := ctx.Err(); err != nil {
			p.Log.Info("worker_stop", "source", s.Source, "stream", s.Name, "consumer", consumerID)
			return
		}
		if err := st.limiter.Wait(ctx); err != nil {
			return
		}
		streams, err := st.cb.Execute(func() ([]redis.XStream, error) {
			return cli.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    s.ConsumerGroup,
				Consumer: consumerID,
				Streams:  []string{s.Name, ">"},
				Count:    int64(s.BatchSize),
				Block:    time.Duration(s.BlockMs) * time.Millisecond,
				NoAck:    false,
			}).Result()
		})
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			if errors.Is(err, gobreaker.ErrOpenState) {
				// Redis деградований — CB Open. Чекаємо open_timeout.
				select {
				case <-time.After(p.Cfg.Breaker.OpenTimeout):
				case <-ctx.Done():
					return
				}
				continue
			}
			p.Log.Error("xreadgroup_error", "source", s.Source, "stream", s.Name, "err", err.Error())
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		for _, xs := range streams {
			if len(xs.Messages) == 0 {
				continue
			}
			metrics.BatchSize.WithLabelValues(streamLabel(s)).Observe(float64(len(xs.Messages)))
			metrics.MessagesRead.WithLabelValues(streamLabel(s)).Add(float64(len(xs.Messages)))
			p.processBatch(ctx, &s, cli, xs.Messages)
		}
	}
}

// streamLabel — "source:stream" для Prometheus labels.
func streamLabel(s mapping.Stream) string { return s.Source + ":" + s.Name }

// entryPrep — розпарсене повідомлення + row.
type entryPrep struct {
	msg    redis.XMessage
	raw    map[string]string
	row    []any
	badErr error
}

// ProcessClaimed — обробляє повідомлення, підібрані reclaim'ом (XAUTOCLAIM),
// тим самим шляхом що і основний worker (convert → INSERT → dead_letters →
// XACK+XDEL лише для збережених). Викликається з reclaim.Loop як Processor.
func (p *Pipeline) ProcessClaimed(ctx context.Context, s *mapping.Stream, cli *redis.Client, msgs []redis.XMessage) {
	if len(msgs) == 0 {
		return
	}
	p.processBatch(ctx, s, cli, msgs)
}

// processBatch — convert → INSERT (з binary-split при частковому збої) → XACK+XDEL.
//
// Гарантія збереження: XACK+XDEL виконується ЛИШЕ для повідомлень, які надійно
// збережені — вставлені в цільову таблицю або записані в dead_letters. Якщо
// Postgres недоступний (circuit breaker Open) або запис у dead_letters не вдався
// — відповідні повідомлення НЕ видаляються з Redis і лишаються в PEL; їх пізніше
// повторно обробить reclaim (XAUTOCLAIM). Так дані не втрачаються під час збою
// Postgres.
func (p *Pipeline) processBatch(ctx context.Context, s *mapping.Stream, cli *redis.Client, msgs []redis.XMessage) {
	preps := make([]entryPrep, len(msgs))
	cols := p.columns(s)
	for i, m := range msgs {
		raw := stringifyValues(m.Values)
		row, err := p.buildRow(s, m.ID, raw)
		preps[i] = entryPrep{msg: m, raw: raw, row: row, badErr: err}
	}

	goodRows := make([][]any, 0, len(preps))
	goodIdx := make([]int, 0, len(preps))
	dead := make([]storage.DeadLetter, 0)
	// ackIDs — тільки надійно збережені повідомлення (persisted або dead-lettered).
	ackIDs := make([]string, 0, len(msgs))

	for i := range preps {
		if preps[i].badErr != nil {
			dead = append(dead, storage.DeadLetter{
				Stream: s.Name, RedisID: preps[i].msg.ID,
				RawFields: p.deadLetterPayload(preps[i].raw),
				Error:     "convert: " + preps[i].badErr.Error(),
			})
			metrics.MessagesDeadLetter.WithLabelValues(streamLabel(*s), "convert").Inc()
			continue
		}
		goodRows = append(goodRows, preps[i].row)
		goodIdx = append(goodIdx, i)
	}

	// Batch INSERT з circuit breaker + retry; при частковому збої — binary split.
	if len(goodRows) > 0 {
		err := p.insertWithRetry(ctx, s, cols, goodRows)
		switch {
		case err == nil:
			metrics.MessagesInserted.WithLabelValues(streamLabel(*s), s.Table).Add(float64(len(goodRows)))
			for _, gi := range goodIdx {
				ackIDs = append(ackIDs, preps[gi].msg.ID)
			}
		case errors.Is(err, gobreaker.ErrOpenState):
			// Postgres недоступний (breaker Open). Нічого не ack'аємо — увесь
			// batch лишається в Redis PEL, XAUTOCLAIM забере його пізніше.
			metrics.MessagesDeferred.WithLabelValues(streamLabel(*s), "pg_open").Add(float64(len(msgs)))
			p.Log.Warn("postgres_unavailable_batch_deferred",
				"source", s.Source, "stream", s.Name, "count", len(msgs))
			return
		case !isPermanentDataError(err):
			// Транзиентний збій (мережа/дедлок/недоступність/відсутня схема) —
			// НЕ ідемо в dead_letters. Увесь batch лишаємо в Redis PEL, повторить
			// XAUTOCLAIM. Так хороші дані не потрапляють у брак через тимчасовий збій.
			metrics.MessagesDeferred.WithLabelValues(streamLabel(*s), "transient").Add(float64(len(msgs)))
			p.Log.Warn("insert_transient_error_deferred",
				"source", s.Source, "stream", s.Name, "count", len(msgs), "err", err.Error())
			return
		default:
			// Помилка від самих даних — binary split ізолює конкретні биті рядки;
			// повертає ID успішно вставлених і список dead_letters.
			settled, perRowDead := p.binarySplitInsert(ctx, s, cols, goodRows, goodIdx, preps)
			ackIDs = append(ackIDs, settled...)
			dead = append(dead, perRowDead...)
		}
	}

	// dead_letters записуємо ДО ack. Ack'аємо dead-повідомлення лише якщо запис
	// удався; інакше лишаємо їх у Redis (не втрачаємо).
	if len(dead) > 0 {
		if err := p.Store.BatchInsertDeadLetters(ctx, p.Cfg.Pipeline.DeadLetterTable, dead); err != nil {
			p.Log.Error("dead_letters_insert_error", "count", len(dead), "err", err.Error())
		} else {
			for _, d := range dead {
				ackIDs = append(ackIDs, d.RedisID)
			}
		}
	}

	// XACK + XDEL один pipeline — тільки надійно збережені повідомлення.
	p.ackAndDeleteBatch(ctx, s, cli, ackIDs)
}

// columns — фіксований порядок колонок для INSERT.
func (p *Pipeline) columns(s *mapping.Stream) []string {
	cols := make([]string, 0, len(s.Fields)+1)
	if s.IDColumn != "" {
		cols = append(cols, s.IDColumn)
	}
	for _, f := range s.Fields {
		cols = append(cols, f.Column)
	}
	return cols
}

// buildRow — конвертація полів entry в порядку columns().
func (p *Pipeline) buildRow(s *mapping.Stream, redisID string, values map[string]string) ([]any, error) {
	row := make([]any, 0, len(s.Fields)+1)
	if s.IDColumn != "" {
		row = append(row, redisID)
	}
	for _, f := range s.Fields {
		raw, present := values[f.Redis]
		if !present && f.NotNull {
			return nil, fmt.Errorf("field %s missing (not_null)", f.Redis)
		}
		if !present {
			row = append(row, nil)
			continue
		}
		v, err := Convert(f, raw)
		if err != nil {
			return nil, err
		}
		row = append(row, v)
	}
	return row, nil
}

// isPermanentDataError — true, якщо помилка Postgres спричинена самими даними
// рядка (порушення обмежень / некоректний формат значення), тобто повтор не
// допоможе і рядок треба класти в dead_letters. Транзиентні збої (мережа,
// дедлок, недоступність, відсутня таблиця/колонка) — НЕ permanent: їх треба
// повторювати, лишаючи дані в Redis, а не втрачати їх у dead_letters.
//
//	SQLSTATE class 22 = data_exception, 23 = integrity_constraint_violation.
func isPermanentDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "22", "23":
		return true
	}
	return false
}

// insertErrText — короткий опис помилки INSERT для колонки dead_letters.error
// БЕЗ сирих значень рядка. Для Postgres беремо SQLSTATE + Message, але НЕ Detail
// (Detail часто містить самі значення, напр. "Key (id)=(123) already exists").
func insertErrText(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return "insert: " + pgErr.Code + " " + pgErr.Message
	}
	return "insert: db error"
}

// backoffWithJitter — експоненційний backoff з повним jitter (AWS-стиль):
// base·2^(attempt-1), обмежений стелею, і випадкова величина у [0, d]. Розкид
// не дає всім worker'ам повторювати синхронно і не роздовбувати базу лавиною.
func backoffWithJitter(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	shift := attempt - 1
	if shift > 6 { // стеля множника ×64
		shift = 6
	}
	d := base << uint(shift)
	if max := 30 * time.Second; d > max {
		d = 30 * time.Second
	}
	return time.Duration(rand.Int63n(int64(d) + 1))
}

// deadLetterPayload — тіло для dead_letters.raw_fields. За замовчуванням — повний
// JSON полів. Якщо pipeline.redact_dead_letter_payload=true (чутливі дані) —
// зберігаємо лише SHA-256 і розмір, без самих значень.
func (p *Pipeline) deadLetterPayload(raw map[string]string) []byte {
	b, _ := json.Marshal(raw)
	if !p.Cfg.Pipeline.RedactDeadLetterPayload {
		return b
	}
	sum := sha256.Sum256(b)
	red, _ := json.Marshal(map[string]any{
		"_redacted": true,
		"sha256":    hex.EncodeToString(sum[:]),
		"size":      len(b),
	})
	return red
}

// insertWithRetry — batch INSERT з circuit breaker + N retry з backoff+jitter.
// Не повторює permanent-помилки даних (безсенсово) і Open-стан breaker'а —
// повертає їх одразу caller'у.
func (p *Pipeline) insertWithRetry(ctx context.Context, s *mapping.Stream, cols []string, rows [][]any) error {
	attempts := p.Cfg.Pipeline.InsertRetries
	if attempts <= 0 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			metrics.InsertRetries.WithLabelValues(streamLabel(*s)).Inc()
			select {
			case <-time.After(backoffWithJitter(p.Cfg.Pipeline.InsertRetryBackoff, i)):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		t0 := time.Now()
		_, err := p.pgCB.Execute(func() (struct{}, error) {
			return struct{}{}, p.Store.BatchInsert(ctx, s, cols, rows)
		})
		metrics.InsertDuration.WithLabelValues(streamLabel(*s), s.Table).Observe(time.Since(t0).Seconds())
		if err == nil {
			return nil
		}
		lastErr = err
		if errors.Is(err, gobreaker.ErrOpenState) {
			return err
		}
		if isPermanentDataError(err) {
			return err // биті дані — повтор не допоможе
		}
	}
	return lastErr
}

// binarySplitInsert — при збої batch INSERT ділимо batch навпіл, рекурсивно
// пробуємо кожну половину. Повертає ID успішно вставлених повідомлень
// (settledIDs) і список dead_letters для реально биті entries. Якщо під час
// split Postgres впав (breaker Open) — решта рядків НЕ ack'ається (лишається в
// Redis). Складність — O(k·log n) де k = кількість проблемних entries; для 1
// бракованого entry в batch=100 — 7 повторів замість 100.
func (p *Pipeline) binarySplitInsert(ctx context.Context, s *mapping.Stream, cols []string, rows [][]any, idx []int, preps []entryPrep) (settledIDs []string, dead []storage.DeadLetter) {
	var walk func(offset int, chunk [][]any)
	walk = func(offset int, chunk [][]any) {
		if len(chunk) == 0 {
			return
		}
		err := p.insertWithRetry(ctx, s, cols, chunk)
		if err == nil {
			metrics.MessagesInserted.WithLabelValues(streamLabel(*s), s.Table).Add(float64(len(chunk)))
			for k := 0; k < len(chunk); k++ {
				settledIDs = append(settledIDs, preps[idx[offset+k]].msg.ID)
			}
			return
		}
		if errors.Is(err, gobreaker.ErrOpenState) {
			// Postgres впав під час split — лишаємо решту в Redis (не ack).
			return
		}
		if len(chunk) == 1 {
			pi := preps[idx[offset]]
			if !isPermanentDataError(err) {
				// Транзиентна помилка на одному рядку — не в dead_letters,
				// лишаємо в Redis (не ack), повторить XAUTOCLAIM.
				metrics.MessagesDeferred.WithLabelValues(streamLabel(*s), "transient").Inc()
				return
			}
			dead = append(dead, storage.DeadLetter{
				Stream: s.Name, RedisID: pi.msg.ID,
				RawFields: p.deadLetterPayload(pi.raw),
				Error:     insertErrText(err),
			})
			metrics.MessagesDeadLetter.WithLabelValues(streamLabel(*s), "insert").Inc()
			return
		}
		mid := len(chunk) / 2
		walk(offset, chunk[:mid])
		walk(offset+mid, chunk[mid:])
	}
	walk(0, rows)
	return
}

// ackAndDeleteBatch — один Redis-pipeline: XACK + XDEL для всіх IDs.
func (p *Pipeline) ackAndDeleteBatch(ctx context.Context, s *mapping.Stream, cli *redis.Client, ids []string) {
	if len(ids) == 0 {
		return
	}
	t0 := time.Now()
	pipe := cli.Pipeline()
	pipe.XAck(ctx, s.Name, s.ConsumerGroup, ids...)
	pipe.XDel(ctx, s.Name, ids...)
	if _, err := pipe.Exec(ctx); err != nil {
		p.Log.Error("ack_pipeline_error", "source", s.Source, "stream", s.Name, "count", len(ids), "err", err.Error())
	}
	metrics.AckDuration.WithLabelValues(streamLabel(*s)).Observe(time.Since(t0).Seconds())
}

// stringifyValues — map[string]interface{} → map[string]string.
func stringifyValues(m map[string]interface{}) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		switch t := v.(type) {
		case string:
			out[k] = t
		case []byte:
			out[k] = string(t)
		case int64:
			out[k] = strconv.FormatInt(t, 10)
		case float64:
			out[k] = strconv.FormatFloat(t, 'f', -1, 64)
		default:
			out[k] = fmt.Sprintf("%v", v)
		}
	}
	return out
}
