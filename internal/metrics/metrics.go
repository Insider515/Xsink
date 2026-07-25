// metrics.go — Prometheus метрики xsink.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Реєстр — глобальний prometheus.DefaultRegisterer через promauto.

// MessagesRead — скільки повідомлень прочитано з XREADGROUP.
var MessagesRead = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "xsink_messages_read_total",
	Help: "Кількість повідомлень отриманих з Redis stream (XREADGROUP).",
}, []string{"stream"})

// MessagesInserted — успішні INSERT'и у Postgres.
var MessagesInserted = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "xsink_messages_inserted_total",
	Help: "Кількість повідомлень успішно вставлених у Postgres.",
}, []string{"stream", "table"})

// MessagesDeadLetter — потрапили в dead_letters.
var MessagesDeadLetter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "xsink_messages_dead_letter_total",
	Help: "Кількість повідомлень що потрапили в dead_letters.",
}, []string{"stream", "reason"})

// MessagesReclaimed — забрані XAUTOCLAIM з PEL іншого consumer'а.
var MessagesReclaimed = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "xsink_messages_reclaimed_total",
	Help: "Скільки stuck-повідомлень підібрано XAUTOCLAIM.",
}, []string{"stream"})

// MessagesDeferred — повідомлення, які НЕ ack'нуто через транзиентний збій
// (reason=transient) або недоступність Postgres (reason=pg_open) і лишено в
// Redis для повторної обробки. Зростання = деградація downstream, не втрата.
var MessagesDeferred = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "xsink_messages_deferred_total",
	Help: "Повідомлення, відкладені (лишені в Redis) через тимчасовий збій.",
}, []string{"stream", "reason"})

// DeadLettersPurged — скільки старих записів видалено з dead_letters по TTL.
var DeadLettersPurged = promauto.NewCounter(prometheus.CounterOpts{
	Name: "xsink_dead_letters_purged_total",
	Help: "Кількість записів dead_letters, видалених за retention (TTL).",
})

// BatchSize — розподіл фактичних розмірів batch'а (може бути < BatchSize з config).
var BatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "xsink_batch_size",
	Help:    "Фактичний розмір batch'а XREADGROUP.",
	Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1..2048
}, []string{"stream"})

// InsertDuration — час на INSERT одного batch'а (сек).
var InsertDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "xsink_insert_duration_seconds",
	Help:    "Тривалість batch INSERT у Postgres.",
	Buckets: prometheus.DefBuckets,
}, []string{"stream", "table"})

// AckDuration — час на XACK+XDEL pipeline.
var AckDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "xsink_ack_duration_seconds",
	Help:    "Тривалість XACK+XDEL pipeline у Redis.",
	Buckets: prometheus.DefBuckets,
}, []string{"stream"})

// InsertRetries — скільки разів INSERT повторювався (0 = успіх з першого разу).
var InsertRetries = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "xsink_insert_retries_total",
	Help: "Скільки INSERT-повторів було виконано.",
}, []string{"stream"})

// PendingCount — довжина PEL (XPENDING) на момент опитування.
var PendingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "xsink_pending_count",
	Help: "Довжина PEL (Pending Entries List) consumer group.",
}, []string{"stream", "group"})

// StreamLength — фактична довжина Redis stream (XLEN).
var StreamLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "xsink_stream_length",
	Help: "XLEN — скільки entries залишилось у stream (нижче = краще).",
}, []string{"stream"})

// BreakerState — стан circuit breaker: 0=Closed, 1=HalfOpen, 2=Open.
var BreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "xsink_breaker_state",
	Help: "Стан circuit breaker: 0=Closed 1=HalfOpen 2=Open.",
}, []string{"component"})

// PostgresPoolInUse — активні conns у пулі pgx.
var PostgresPoolInUse = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "xsink_postgres_pool_in_use",
	Help: "Кількість активних з'єднань у пулі pgxpool.",
})

// PostgresPoolIdle — простійні conns.
var PostgresPoolIdle = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "xsink_postgres_pool_idle",
	Help: "Кількість idle-з'єднань у пулі pgxpool.",
})
