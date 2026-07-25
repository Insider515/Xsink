// reclaim.go — фонова горутина XAUTOCLAIM (multi-source).
//
// Забирає stuck-повідомлення з PEL іншого consumer'а (краш, довгий stall)
// у контексті кожного stream'а на його source-клієнті.
package reclaim

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"xsink/internal/config"
	"xsink/internal/mapping"
	"xsink/internal/metrics"
)

// Processor — колбек, що переобробляє підібрані XAUTOCLAIM повідомлення (INSERT
// у Postgres + XACK+XDEL при успіху). Передається pipeline.ProcessClaimed.
// Без нього XAUTOCLAIM лише змінює власника в PEL, але повідомлення не
// зберігаються і не видаляються — тобто зависають назавжди.
type Processor func(ctx context.Context, s *mapping.Stream, cli *redis.Client, msgs []redis.XMessage)

// Loop — блокуюча функція; викликається з окремої горутини у main.
// clients: map[source_name]*redis.Client, streams: усі stream'и (з різних sources).
// proc: обробник підібраних повідомлень (може бути nil).
func Loop(ctx context.Context, clients map[string]*redis.Client, streams []mapping.Stream, cfg config.ReclaimConfig, log *slog.Logger, proc Processor) {
	if !cfg.Enabled {
		log.Info("reclaim_disabled")
		return
	}
	hostname, _ := os.Hostname()
	consumerID := "reclaim-" + hostname
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	log.Info("reclaim_start",
		"interval", cfg.Interval.String(),
		"min_idle", cfg.MinIdleTime.String(),
		"batch", cfg.BatchSize)
	for {
		select {
		case <-ctx.Done():
			log.Info("reclaim_stop")
			return
		case <-t.C:
		}
		for _, s := range streams {
			cli, ok := clients[s.Source]
			if !ok {
				continue
			}
			runOne(ctx, cli, s, consumerID, cfg, log, proc)
		}
	}
}

// runOne — XAUTOCLAIM для одного stream на його source-клієнті. Підібрані
// повідомлення передаються proc для повторної обробки (INSERT + XACK+XDEL).
func runOne(ctx context.Context, cli *redis.Client, s mapping.Stream, consumerID string, cfg config.ReclaimConfig, log *slog.Logger, proc Processor) {
	start := "0-0"
	for {
		msgs, next, err := cli.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   s.Name,
			Group:    s.ConsumerGroup,
			Consumer: consumerID,
			MinIdle:  cfg.MinIdleTime,
			Start:    start,
			Count:    int64(cfg.BatchSize),
		}).Result()
		if err != nil {
			log.Error("xautoclaim_error", "source", s.Source, "stream", s.Name, "err", err.Error())
			return
		}
		if len(msgs) > 0 {
			metrics.MessagesReclaimed.WithLabelValues(s.Source + ":" + s.Name).Add(float64(len(msgs)))
			log.Info("reclaim_batch", "source", s.Source, "stream", s.Name, "count", len(msgs))
			if proc != nil {
				st := s
				proc(ctx, &st, cli, msgs)
			}
		}
		if next == "0-0" || next == "" {
			return
		}
		start = next
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// PendingObserver — фонова петля, що періодично оновлює gauge'и XPENDING і XLEN
// по всіх stream'ах з відповідних source-клієнтів.
func PendingObserver(ctx context.Context, clients map[string]*redis.Client, streams []mapping.Stream, interval time.Duration, log *slog.Logger) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, s := range streams {
			cli, ok := clients[s.Source]
			if !ok {
				continue
			}
			label := s.Source + ":" + s.Name
			if p, err := cli.XPending(ctx, s.Name, s.ConsumerGroup).Result(); err == nil && p != nil {
				metrics.PendingCount.WithLabelValues(label, s.ConsumerGroup).Set(float64(p.Count))
			}
			if n, err := cli.XLen(ctx, s.Name).Result(); err == nil {
				metrics.StreamLength.WithLabelValues(label).Set(float64(n))
			}
		}
	}
}
